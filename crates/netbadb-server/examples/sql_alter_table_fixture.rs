//! Fresh runtime-created Heap exposed to real PostgreSQL ALTER clients.
use netbadb_core::{Database, DatabaseCoordinatorConfig, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde_json::json;
use std::error::Error;
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "netbadb-round26-driver-{}-{nonce}",
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
    let mut db = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(
            root.join("seed"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )?;
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")?;
    db.execute("CREATE INDEX projects_name_idx ON projects (name)")?;
    db.execute("INSERT INTO projects VALUES (1, 'one')")?;
    db.close()?;

    let runtime_directory = std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .find(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("catalog.resources-")
        })
        .ok_or("runtime storage directory missing")?;
    let project_path = runtime_directory.path().join("storage/2.heap");
    let project_relative = project_path
        .strip_prefix(root)?
        .to_str()
        .ok_or("runtime storage path is not UTF-8")?;

    let manifest = root.join("server.json");
    let config = serde_json::to_vec_pretty(&json!({
        "version": 4,
        "listen": "127.0.0.1:0",
        "authorization": {"local_plaintext": {"schema_admin": true, "tables": [
            {"table_id": 2, "read": true, "write": true, "transaction": true, "analyze": false}
        ]}, "clients": []},
        "tables": [
            {"path": "seed", "id": 1, "name": "seed", "columns": [
                {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false}
            ]},
            {"path": project_relative, "id": 2, "name": "projects", "columns": [
                {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
                {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
            ]}
        ]
    }))?;
    std::fs::write(&manifest, &config)?;
    let server = PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest)?).start()?;
    println!("{}", server.local_addr());
    io::stdout().flush()?;
    io::stdin().read_to_end(&mut Vec::new())?;
    server.shutdown()?;
    if std::fs::read(&manifest)? != config {
        return Err("SQL ALTER changed the manifest".into());
    }
    let probe = std::env::var("NETBADB_ROUND30_PROBE")?;
    let (base, required, absent) = match probe.as_str() {
        "psql_probe" => (Some("projects"), "psql_composed", Some("psql_noop")),
        "psycopg_probe" => (Some("projects"), "psycopg_composed", None),
        "sqlalchemy_probe" => (Some("work"), "sqlalchemy_composed", Some("sqlalchemy_noop")),
        "alembic_probe" => (None, "alembic_composed", Some("alembic_noop")),
        _ => return Err(format!("unknown Round 30 probe {probe}").into()),
    };
    for _ in 0..3 {
        let reopened = Database::open_catalog(&catalog)?;
        let composed = reopened
            .schema()
            .table(required)
            .ok_or("client-created table disappeared after reopen")?;
        if reopened.table_schema_version(composed.id).is_none() || composed.columns.is_empty() {
            return Err("composed table identity/version is invalid after reopen".into());
        }
        if let Some(base) = base {
            let table = reopened
                .schema()
                .table(base)
                .ok_or("base table disappeared after table-object composition")?;
            if reopened.table_schema_version(table.id).is_none() || table.columns.is_empty() {
                return Err("base table identity/version is invalid after reopen".into());
            }
        }
        if absent.is_some_and(|name| reopened.schema().table(name).is_some()) {
            return Err("CREATE-to-DROP no-op table survived reopen".into());
        }
        reopened.close()?;
    }
    println!(
        "REOPEN PASS: {probe} final table-object schema survived three catalog-only opens; transient no-op absent; manifest unchanged"
    );
    Ok(())
}
