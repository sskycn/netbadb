use netbadb_core::Database;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn users() -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

#[test]
fn public_embedded_api_round_trips_rows_and_queries() {
    let path = std::env::temp_dir().join(format!("netbadb-integration-{}", std::process::id()));
    let mut database = Database::create(&path, users()).expect("create database");
    database
        .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
        .expect("insert");

    let result = database
        .query("SELECT id, name FROM users WHERE id = 1 LIMIT 1")
        .expect("query");
    assert_eq!(
        result.rows,
        vec![vec![ScalarValue::Int64(1), ScalarValue::Text("Ada".into())]]
    );
    drop(database);
    let _ = std::fs::remove_file(path);
}
