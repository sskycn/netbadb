use netbadb_core::{Database, DatabaseErrorKind, DdlOutcome, ExecutionResult};
use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn ddl(database: &mut Database, sql: &str) -> Result<DdlOutcome, netbadb_core::DatabaseError> {
    let prepared = database.prepare_ddl_statement(sql)?;
    database.execute_ddl(&prepared)
}

fn has_path(node: &PlanNodeInspection, join: bool) -> bool {
    match node {
        PlanNodeInspection::IndexNestedLoopJoin { .. } if join => true,
        PlanNodeInspection::IndexScan { .. } | PlanNodeInspection::RangeIndexScan { .. }
            if !join =>
        {
            true
        }
        PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Limit { input, .. }
        | PlanNodeInspection::Aggregate { input, .. } => has_path(input, join),
        PlanNodeInspection::HashJoin { left, right, .. }
        | PlanNodeInspection::NestedLoopJoin { left, right, .. } => {
            has_path(left, join) || has_path(right, join)
        }
        PlanNodeInspection::IndexNestedLoopJoin { left, .. } => has_path(left, join),
        _ => false,
    }
}

fn planned(database: &Database, sql: &str, join: bool) -> bool {
    let StatementPlanInspection::Query { root } = database.inspect_statement(sql).unwrap().plan
    else {
        panic!("query");
    };
    has_path(&root, join)
}

#[test]
fn drop_commit_rollback_replan_index_join_and_reopen() {
    let root = std::env::temp_dir().join(format!("netbadb-round7-core-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let tables = vec![
        (root.join("users"), table(1, "users")),
        (root.join("probes"), table(2, "probes")),
    ];
    let mut db = Database::create_tables(tables.clone()).unwrap();
    for id in 0..128 {
        db.insert_into(
            TableId(1),
            &[ScalarValue::Int64(id), ScalarValue::Text("v".repeat(128))],
        )
        .unwrap();
    }
    db.insert_into(
        TableId(2),
        &[ScalarValue::Int64(3), ScalarValue::Text("probe".into())],
    )
    .unwrap();
    ddl(&mut db, "CREATE INDEX users_id_idx ON users (id)").unwrap();
    db.analyze(TableId(1)).unwrap();
    db.analyze(TableId(2)).unwrap();
    let point = "SELECT name FROM users WHERE id = 3";
    let range = "SELECT name FROM users WHERE id >= 3 AND id <= 5";
    let join = "SELECT u.name FROM probes p JOIN users u ON p.id = u.id";
    assert!(planned(&db, point, false));
    assert!(planned(&db, range, false));
    assert!(planned(&db, join, true));
    let prepared = db
        .prepare_statement("SELECT name FROM users WHERE id = $1", &[])
        .unwrap();
    let prepared_join = db.prepare_statement(join, &[]).unwrap();
    let expected = db
        .execute_prepared(&prepared, &[ScalarValue::Int64(3)])
        .unwrap();
    let expected_join = db.execute_prepared(&prepared_join, &[]).unwrap();
    let old = db.indexes(TableId(1)).unwrap()[0].clone();
    let drop = db
        .prepare_ddl_statement("DROP INDEX public.users_id_idx")
        .unwrap();
    assert_eq!(drop.access().write_tables(), vec![TableId(1)]);
    let generation = db.catalog_generation();
    let mut tx = db.begin_transaction_for(TableId(1)).unwrap();
    db.execute_ddl_in(&mut tx, &drop).unwrap();
    db.execute_in(
        &mut tx,
        "INSERT INTO users (id, name) VALUES (1000, 'pending drop')",
    )
    .unwrap();
    assert!(planned(&db, join, true));
    assert_eq!(db.catalog_generation(), generation);
    tx.rollback().unwrap();
    assert_eq!(db.indexes(TableId(1)).unwrap(), std::slice::from_ref(&old));
    assert_eq!(
        ddl(&mut db, "CREATE INDEX users_id_idx ON users (id)")
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::DuplicateObject
    );
    let mut tx = db.begin_transaction_for(TableId(1)).unwrap();
    db.execute_ddl_in(&mut tx, &drop).unwrap();
    assert!(
        tx.commit().is_err(),
        "DDL publication must go through Database"
    );
    db.commit_transaction(&mut tx).unwrap();
    let no_op = db
        .prepare_ddl_statement("DROP INDEX IF EXISTS missing")
        .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut tx, &no_op).unwrap_err().kind(),
        DatabaseErrorKind::TransactionState
    );

    assert_eq!(db.catalog_generation(), generation + 1);
    assert!(db.inspect_catalog().unwrap().tables[0].indexes.is_empty());
    for (sql, is_join) in [(point, false), (range, false), (join, true)] {
        assert!(!planned(&db, sql, is_join), "{sql}");
    }
    assert_eq!(
        db.execute_prepared(&prepared, &[ScalarValue::Int64(3)])
            .unwrap(),
        expected
    );
    assert_eq!(
        db.execute_prepared(&prepared_join, &[]).unwrap(),
        expected_join
    );
    db.execute("INSERT INTO users (id, name) VALUES (2000, 'new')")
        .unwrap();
    db.execute("UPDATE users SET id = 2001 WHERE id = 2000")
        .unwrap();
    db.execute("DELETE FROM users WHERE id = 2001").unwrap();
    db.analyze(TableId(1)).unwrap();
    db.vacuum(TableId(1)).unwrap();
    assert!(!planned(&db, join, true));
    assert_eq!(
        ddl(&mut db, "DROP INDEX IF EXISTS users_id_idx").unwrap(),
        DdlOutcome::Unchanged
    );
    assert_eq!(
        ddl(&mut db, "DROP INDEX missing").unwrap_err().kind(),
        DatabaseErrorKind::UndefinedObject
    );
    db.checkpoint().unwrap();
    db.close().unwrap();
    for _ in 0..2 {
        let db = Database::open_tables(tables.clone()).unwrap();
        assert!(db.indexes(TableId(1)).unwrap().is_empty());
        db.close().unwrap();
    }
    let mut db = Database::open_tables(tables.clone()).unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (3000, 'backfill')")
        .unwrap();
    ddl(&mut db, "CREATE INDEX users_id_idx ON users (id)").unwrap();
    let new = db.indexes(TableId(1)).unwrap()[0].clone();
    assert_ne!(new.id, old.id);
    assert_ne!(new.handle, old.handle);
    assert!(
        db.inspect_catalog().unwrap().tables[0].indexes[0]
            .statistics
            .is_none()
    );
    assert_eq!(
        db.execute_ddl(&drop).unwrap_err().kind(),
        DatabaseErrorKind::UndefinedObject,
        "stale prepared DDL must not drop a replacement index"
    );
    let ExecutionResult::Query(found) = db
        .execute_prepared(&prepared, &[ScalarValue::Int64(3000)])
        .unwrap()
    else {
        panic!("query");
    };
    assert_eq!(found.rows, vec![vec![ScalarValue::Text("backfill".into())]]);
    let mut tx = db.begin_transaction_for(TableId(1)).unwrap();
    let drop_new = db.prepare_ddl_statement("DROP INDEX users_id_idx").unwrap();
    db.execute_ddl_in(&mut tx, &drop_new).unwrap();
    let create = db
        .prepare_ddl_statement("CREATE INDEX users_id_idx ON users (id)")
        .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut tx, &create).unwrap_err().kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    tx.rollback().unwrap();
    ddl(&mut db, "DROP INDEX users_id_idx").unwrap();
    let mut tx = db.begin_transaction_for(TableId(1)).unwrap();
    db.execute_ddl_in(&mut tx, &create).unwrap();
    let drop_pending = db
        .prepare_ddl_statement("DROP INDEX IF EXISTS users_id_idx")
        .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut tx, &drop_pending)
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    tx.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
