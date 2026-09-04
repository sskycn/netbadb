//! Fresh runtime-created Heap exposed to real PostgreSQL ALTER clients.
use netbadb_core::{Database, DatabaseCoordinatorConfig, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{PostgresTcpServer, ServerConfig};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
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
    let round36_probe = std::env::var("NETBADB_ROUND36_PROBE").ok();
    let round37_probe = std::env::var("NETBADB_ROUND37_PROBE").ok();
    let round39_probe = std::env::var("NETBADB_ROUND39_PROBE").ok();
    let round42_probe = std::env::var("NETBADB_ROUND42_PROBE").ok();
    let round44_probe = std::env::var("NETBADB_ROUND44_PROBE").ok();
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
    if round42_probe.is_some() || round44_probe.is_some() {
        db.execute("CREATE TABLE projects (id BIGINT NOT NULL, legacy TEXT, email TEXT)")?;
        if round42_probe.is_some() {
            db.execute("CREATE INDEX projects_legacy_idx ON projects (legacy)")?;
        }
        db.execute("CREATE INDEX projects_email_idx ON projects (email)")?;
        db.execute("INSERT INTO projects VALUES (1, 'old-one', NULL)")?;
        db.execute("INSERT INTO projects VALUES (2, 'old-two', 'two@example.test')")?;
        db.execute("INSERT INTO projects VALUES (3, 'old-three', 'three@example.test')")?;
    } else {
        db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")?;
        db.execute("CREATE INDEX projects_name_idx ON projects (name)")?;
        db.execute("INSERT INTO projects VALUES (1, 'one')")?;
        if round36_probe.is_some() || round37_probe.is_some() || round39_probe.is_some() {
            db.execute("INSERT INTO projects VALUES (2, NULL)")?;
        }
    }
    let old_index_id = db.indexes(TableId(2))?[0].id;
    let base_version = db
        .table_schema_version(TableId(2))
        .ok_or("projects version missing")?;
    let base_generation = db.schema_generation();
    let target_storage = db.next_storage_id().ok_or("StorageId floor missing")?;
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
    let project_columns = if round42_probe.is_some() || round44_probe.is_some() {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "legacy", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false},
            {"id": 3, "name": "email", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
        ])
    } else {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
        ])
    };
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
            {"path": project_relative, "id": 2, "name": "projects", "columns": project_columns}
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
    if let Some(probe) = round44_probe {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let final_name = if probe == "rename-table" {
                "accounts"
            } else {
                "projects"
            };
            let projects = reopened
                .schema()
                .table(final_name)
                .ok_or("Round 44 final table disappeared")?
                .clone();
            let indexes = reopened.indexes(TableId(2))?;
            match probe.as_str() {
                "add" => {
                    if projects.column("marker").map(|column| column.id) != Some(ColumnId(4))
                        || reopened.next_storage_id().map(|storage| storage.0)
                            != Some(target_storage.0 + 1)
                    {
                        return Err("Round 44 ADD result is incorrect".into());
                    }
                }
                "drop" => {
                    if projects.column("legacy").is_some() || indexes.len() != 1 {
                        return Err("Round 44 DROP result is incorrect".into());
                    }
                }
                "rename-column" => {
                    if projects.column("contact").map(|column| column.id) != Some(ColumnId(3))
                        || indexes.len() != 1
                        || indexes[0].id != old_index_id
                        || indexes[0].column_id != ColumnId(3)
                    {
                        return Err("Round 44 column rename result is incorrect".into());
                    }
                }
                "rename-table" => {
                    if projects.id != TableId(2) || reopened.schema().table("projects").is_some() {
                        return Err("Round 44 table rename result is incorrect".into());
                    }
                }
                "multiple-add" => {
                    if projects.column("marker").map(|column| column.id) != Some(ColumnId(4))
                        || projects.column("score").map(|column| column.id) != Some(ColumnId(5))
                    {
                        return Err("Round 44 multiple ADD result is incorrect".into());
                    }
                }
                "same-name" => {
                    if projects.column("legacy").map(|column| column.id) != Some(ColumnId(4)) {
                        return Err("Round 44 same-name replacement is incorrect".into());
                    }
                }
                "noop" => {
                    if projects.column("temporary").is_some()
                        || reopened.table_schema_version(TableId(2)) != Some(base_version)
                        || reopened.schema_generation() != base_generation
                        || reopened.next_storage_id() != Some(target_storage)
                        || reopened.next_column_id(TableId(2)) != Some(ColumnId(5))
                    {
                        return Err("Round 44 no-op result is incorrect".into());
                    }
                }
                "mixed" => {
                    let rows = reopened
                        .query("SELECT id, contact, marker FROM projects ORDER BY id")?
                        .rows;
                    if projects.column("legacy").is_some()
                        || projects.column("contact").map(|column| column.id) != Some(ColumnId(3))
                        || projects.column("marker").map(|column| column.id) != Some(ColumnId(4))
                        || rows
                            != [
                                vec![
                                    ScalarValue::Int64(1),
                                    ScalarValue::Text("filled@example.test".into()),
                                    ScalarValue::Null,
                                ],
                                vec![
                                    ScalarValue::Int64(3),
                                    ScalarValue::Text("three@example.test".into()),
                                    ScalarValue::Null,
                                ],
                                vec![
                                    ScalarValue::Int64(4),
                                    ScalarValue::Text("four@example.test".into()),
                                    ScalarValue::Null,
                                ],
                            ]
                    {
                        return Err("Round 44 mixed DML result is incorrect".into());
                    }
                }
                "rollback" | "set-not-null" | "dml-after" | "indexed-drop" | "index-after"
                | "cross-table" => {
                    if projects.columns.len() != 3
                        || reopened.table_schema_version(TableId(2)) != Some(base_version)
                        || reopened.schema_generation() != base_generation
                        || reopened.next_storage_id() != Some(target_storage)
                    {
                        return Err("Round 44 rollback/negative result is incorrect".into());
                    }
                }
                _ => return Err(format!("unknown Round 44 probe {probe}").into()),
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 44 post-DML adoption result survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round42_probe {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 42 projects table disappeared")?
                .clone();
            let rows = reopened.query("SELECT * FROM projects ORDER BY id")?.rows;
            let indexes = reopened.indexes(TableId(2))?;
            match probe.as_str() {
                "combined" => {
                    if projects.column("legacy").map(|column| column.id) != Some(ColumnId(4))
                        || projects.column("contact").map(|column| column.id) != Some(ColumnId(3))
                        || projects
                            .column("contact")
                            .is_none_or(|column| column.nullable)
                        || indexes.len() != 1
                        || indexes[0].column_id != ColumnId(3)
                        || rows
                            != [
                                vec![
                                    ScalarValue::Int64(1),
                                    ScalarValue::Text("filled@example.test".into()),
                                    ScalarValue::Null,
                                ],
                                vec![
                                    ScalarValue::Int64(3),
                                    ScalarValue::Text("three@example.test".into()),
                                    ScalarValue::Null,
                                ],
                                vec![
                                    ScalarValue::Int64(4),
                                    ScalarValue::Text("four@example.test".into()),
                                    ScalarValue::Null,
                                ],
                            ]
                    {
                        return Err("Round 42 combined result is incorrect".into());
                    }
                }
                "add" => {
                    if projects.column("marker").map(|column| column.id) != Some(ColumnId(4))
                        || rows
                            .iter()
                            .any(|row| row.last() != Some(&ScalarValue::Null))
                    {
                        return Err("Round 42 ADD result is incorrect".into());
                    }
                }
                "drop" => {
                    if projects.column("legacy").is_some() || projects.columns.len() != 2 {
                        return Err("Round 42 DROP result is incorrect".into());
                    }
                }
                "multiple-add" => {
                    if projects.column("marker").map(|column| column.id) != Some(ColumnId(4))
                        || projects.column("score").map(|column| column.id) != Some(ColumnId(5))
                        || rows.iter().any(|row| {
                            row.get(3) != Some(&ScalarValue::Null)
                                || row.get(4) != Some(&ScalarValue::Null)
                        })
                    {
                        return Err("Round 42 multiple ADD result is incorrect".into());
                    }
                }
                "noop" => {
                    if projects.column("temporary").is_some()
                        || projects.columns.len() != 3
                        || reopened.table_schema_version(TableId(2)) != Some(base_version)
                        || reopened.schema_generation() != base_generation
                        || reopened.next_storage_id() != Some(target_storage)
                    {
                        return Err("Round 42 ADD/DROP no-op result is incorrect".into());
                    }
                }
                "rollback" | "post-dml" | "read-only-add" | "pending-index" | "new-index"
                | "new-not-null" => {
                    if projects.columns.len() != 3
                        || projects.column("legacy").map(|column| column.id) != Some(ColumnId(2))
                        || indexes.len() != 2
                        || rows.len() != 3
                        || reopened.next_storage_id() != Some(target_storage)
                    {
                        return Err("Round 42 rollback/negative result is incorrect".into());
                    }
                }
                _ => return Err(format!("unknown Round 42 probe {probe}").into()),
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 42 layout result survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round39_probe {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let name_nullable = reopened
                .schema()
                .table("projects")
                .and_then(|table| table.column("name"))
                .ok_or("Round 39 projects.name disappeared")?
                .nullable;
            let indexes = reopened.indexes(TableId(2))?.to_vec();
            let rows = reopened
                .query("SELECT id, name FROM projects ORDER BY id")?
                .rows;
            match probe.as_str() {
                "replacement" | "no-replacement" => {
                    if name_nullable
                        || reopened
                            .table_schema_version(TableId(2))
                            .map(|version| version.0)
                            != Some(base_version.0 + 1)
                        || reopened.schema_generation().0 != base_generation.0 + 1
                        || reopened.next_storage_id().map(|storage| storage.0)
                            != Some(target_storage.0 + 1)
                        || rows
                            != [
                                vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                                vec![ScalarValue::Int64(2), ScalarValue::Text("filled".into())],
                            ]
                    {
                        return Err("Round 39 final schema/identity/rows are incorrect".into());
                    }
                    if probe == "replacement"
                        && (indexes.len() != 1
                            || indexes[0].id == old_index_id
                            || indexes[0].column_id != ColumnId(2)
                            || indexes[0]
                                .name
                                .as_ref()
                                .is_none_or(|name| name.as_str() != "projects_name_idx"))
                    {
                        return Err("Round 39 replacement index identity is incorrect".into());
                    }
                    if probe == "no-replacement" && !indexes.is_empty() {
                        return Err("Round 39 no-replacement migration kept an index".into());
                    }
                    let retired = reopened.inspect_replacement_retired_heaps();
                    if retired.len() != 1
                        || retired[0].table_id != TableId(2)
                        || retired[0].new_storage_id != target_storage
                    {
                        return Err("Round 39 source retirement evidence is incorrect".into());
                    }
                }
                "no-alter" | "net-noop" => {
                    if !name_nullable
                        || reopened.table_schema_version(TableId(2)) != Some(base_version)
                        || reopened.schema_generation() != base_generation
                        || reopened.next_storage_id() != Some(target_storage)
                        || !indexes.is_empty()
                        || !reopened.inspect_replacement_retired_heaps().is_empty()
                        || rows
                            != [
                                vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                                vec![ScalarValue::Int64(2), ScalarValue::Text("filled".into())],
                            ]
                    {
                        return Err("Round 39 ordinary/no-op source result is incorrect".into());
                    }
                }
                "partial" | "dml-after" | "rollback" => {
                    if !name_nullable
                        || reopened.table_schema_version(TableId(2)) != Some(base_version)
                        || reopened.schema_generation() != base_generation
                        || reopened.next_storage_id() != Some(target_storage)
                        || indexes.len() != 1
                        || indexes[0].id != old_index_id
                        || rows
                            != [
                                vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                                vec![ScalarValue::Int64(2), ScalarValue::Null],
                            ]
                    {
                        return Err("Round 39 rollback did not restore the base".into());
                    }
                }
                _ => return Err(format!("unknown Round 39 probe {probe}").into()),
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 39 DROP-first result survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round37_probe {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 37 baseline table disappeared")?;
            if projects
                .column("name")
                .is_none_or(|column| !column.nullable)
            {
                return Err("Round 37 baseline changed the logical schema".into());
            }
            let indexes = reopened.indexes(TableId(2))?;
            let expected_rows = match probe.as_str() {
                "ordinary" if indexes.is_empty() => [
                    vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                    vec![ScalarValue::Int64(2), ScalarValue::Text("filled".into())],
                ],
                "negative" if indexes.len() == 1 && indexes[0].id == old_index_id => [
                    vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                    vec![ScalarValue::Int64(2), ScalarValue::Null],
                ],
                "ordinary" => {
                    return Err("Round 37 ordinary commit kept the dropped index".into());
                }
                "negative" => {
                    return Err("Round 37 failed migration did not restore Iold".into());
                }
                _ => return Err(format!("unknown Round 37 probe {probe}").into()),
            };
            if reopened
                .query("SELECT id, name FROM projects ORDER BY id")?
                .rows
                != expected_rows
            {
                return Err("Round 37 baseline rows are incorrect".into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 37 DROP-first baseline survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round36_probe {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 36 final table disappeared")?;
            if projects.column("migration_marker").is_none()
                || projects.column("name").is_none_or(|column| column.nullable)
            {
                return Err("Round 36 final nullability is incorrect".into());
            }
            let indexes = reopened.indexes(TableId(2))?;
            match probe.as_str() {
                "replacement" => {
                    if indexes.len() != 1
                        || indexes[0].id == old_index_id
                        || indexes[0]
                            .name
                            .as_ref()
                            .is_none_or(|name| name.as_str() != "projects_name_idx")
                        || indexes[0].column_id != ColumnId(2)
                    {
                        return Err("Round 36 replacement index identity is incorrect".into());
                    }
                }
                "no-replacement" if indexes.is_empty() => {}
                "no-replacement" => {
                    return Err("Round 36 no-replacement migration kept an index".into());
                }
                _ => return Err(format!("unknown Round 36 probe {probe}").into()),
            }
            if reopened
                .query("SELECT id, name, migration_marker FROM projects ORDER BY id")?
                .rows
                != [
                    vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Text("one".into()),
                        ScalarValue::Text("done".into()),
                    ],
                    vec![
                        ScalarValue::Int64(2),
                        ScalarValue::Text("filled".into()),
                        ScalarValue::Text("done".into()),
                    ],
                ]
            {
                return Err("Round 36 final rows are incorrect".into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} staged indexed-nullability schema/data/index survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Ok(probe) = std::env::var("NETBADB_ROUND32_PROBE") {
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 32 final table disappeared")?;
            if !projects
                .column("normalized_name")
                .is_some_and(|column| !column.nullable)
                || projects.columns.len() != 3
            {
                return Err("failed Round 32 transaction published final schema".into());
            }
            if reopened
                .query("SELECT id, name, normalized_name FROM projects ORDER BY id")?
                .rows
                != [vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("one".into()),
                    ScalarValue::Text("filled".into()),
                ]]
            {
                return Err("failed Round 32 transaction changed final rows".into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} final schema/data and private retarget survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
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
