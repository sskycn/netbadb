//! Fresh authorized SQL DDL endpoint; shutdown verifies catalog-only durability.
use netbadb_core::{Database, SchemaGeneration, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId, TableSchemaVersion};
use serde_json::json;
use std::error::Error;
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "netbadb-round19-driver-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let result = run(&root);
    if result.is_ok() {
        std::fs::remove_dir_all(&root)?;
    } else {
        eprintln!("failed fixture retained at {}", root.display());
    }
    result
}

fn run(root: &Path) -> Result<(), Box<dyn Error>> {
    let catalog = root.join("catalog");
    Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(
            root.join("users"),
            TableDef::new(
                TableId(1),
                "users",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        None,
    )?
    .close()?;
    let manifest = root.join("server.json");
    let config = serde_json::to_vec_pretty(&json!({
        "version": 5, "listen": "127.0.0.1:0",
        "authorization": {"local_plaintext": {"schema_admin": true, "tables": []}, "clients": []},
        "tables": [{"path": "users", "id": 1, "name": "users", "columns": [
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false}
        ]}]
    }))?;
    std::fs::write(&manifest, &config)?;
    let server = PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
    println!("{}", server.local_addr());
    io::stdout().flush()?;
    io::stdin().read_to_end(&mut Vec::new())?;
    server.shutdown()?;
    if std::fs::read(&manifest)? != config {
        return Err("SQL changed the manifest".into());
    }
    // Startup expectations remain the original single users table, never rebuilt
    // from dynamic SQL. Also exercise the normal server restart with that subset.
    let server = PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
    server.shutdown()?;
    let mut db = Database::open_catalog(&catalog)?;
    if db.schema().table("rolled_back").is_some() {
        return Err("rollback published a table".into());
    }
    let table = db
        .schema()
        .table("committed")
        .ok_or("committed table missing")?
        .clone();
    if table.id != TableId(3)
        || db.schema_generation() != SchemaGeneration(2)
        || db.table_schema_version(table.id) != Some(TableSchemaVersion(1))
    {
        return Err("unexpected identity/generation/version after reopen".into());
    }
    if db.query("SELECT * FROM committed")?.rows
        != vec![vec![
            ScalarValue::Int64(10),
            ScalarValue::Text("demo".into()),
            ScalarValue::Bool(true),
        ]]
    {
        return Err("committed rows changed after catalog-only reopen".into());
    }
    if table.columns.iter().map(|c| c.nullable).collect::<Vec<_>>() != [false, true, false]
        || table
            .columns
            .iter()
            .any(|c| c.primary_key || c.semantic_type().name.is_some())
    {
        return Err("SQL column semantics changed".into());
    }
    println!(
        "REOPEN PASS: rollback=(TableId 2, StorageId 2); commit=TableId 3; generation=2; table_version=1; rows=(10,demo,true); original manifest unchanged"
    );
    db.close()?;
    Ok(())
}
