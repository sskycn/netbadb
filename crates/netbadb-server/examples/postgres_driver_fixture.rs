use std::error::Error;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use netbadb_core::Database;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde_json::json;

fn main() -> Result<(), Box<dyn Error>> {
    let directory = fixture_directory()?;
    std::fs::create_dir(&directory)?;
    let result = run(&directory);
    let _ = std::fs::remove_dir_all(&directory);
    result
}

fn run(directory: &Path) -> Result<(), Box<dyn Error>> {
    let tables = vec![
        (directory.join("users.ndb"), users_table()),
        (directory.join("teams.ndb"), teams_table()),
    ];
    let mut database = Database::create_tables(tables.clone())?;
    database.create_index(TableId(1), ColumnId(2))?;
    database.create_index(TableId(1), ColumnId(3))?;
    database.close()?;
    let reopened = Database::open_tables(tables)?;
    let indexes = &reopened.inspect_catalog()?.tables[0].indexes;
    if indexes.len() != 2 {
        return Err(format!(
            "expected two reopened users indexes, found {}",
            indexes.len()
        )
        .into());
    }
    reopened.close()?;
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        serde_json::to_vec_pretty(&json!({
            "version": 4,
            "listen": "127.0.0.1:0",
            "authorization": {
                "local_plaintext": {"tables": [
                    {
                        "table_id": 1,
                        "read": true,
                        "write": true,
                        "transaction": true,
                        "analyze": false
                    },
                    {
                        "table_id": 2,
                        "read": true,
                        "write": true,
                        "transaction": true,
                        "analyze": false
                    }
                ]},
                "clients": []
            },
            "tables": [{
                "path": "users.ndb",
                "id": 1,
                "name": "users",
                "columns": [
                    {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": "UserId", "nullable": false, "primary_key": true},
                    {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false},
                    {"id": 3, "name": "active", "physical_type": "bool", "semantic_type": null, "nullable": false, "primary_key": false}
                ]
            }, {
                "path": "teams.ndb",
                "id": 2,
                "name": "teams",
                "columns": [
                    {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": "TeamId", "nullable": false, "primary_key": true},
                    {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": false, "primary_key": false}
                ]
            }]
        }))?,
    )?;
    let server = PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
    println!("{}", server.local_addr());
    io::stdout().flush()?;
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    server.shutdown()?;
    Ok(())
}

fn teams_table() -> TableDef {
    TableDef::new(
        TableId(2),
        "teams",
        vec![
            ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Semantic {
                    name: "TeamId".into(),
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn users_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Semantic {
                    name: "UserId".into(),
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
            ColumnDef::new(
                ColumnId(3),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
        ],
    )
}

fn fixture_directory() -> Result<PathBuf, Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "netbadb-postgres-driver-{}-{nonce}",
        std::process::id()
    )))
}
