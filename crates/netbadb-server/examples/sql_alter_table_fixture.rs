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
    let round45_probe = std::env::var("NETBADB_ROUND45_PROBE").ok();
    let round48_probe = std::env::var("NETBADB_ROUND48_PROBE").ok();
    let round50_probe = std::env::var("NETBADB_ROUND50_PROBE").ok();
    let round52_probe = std::env::var("NETBADB_ROUND52_PROBE").ok();
    let round54_probe = std::env::var("NETBADB_ROUND54_PROBE").ok();
    let round56_probe = std::env::var("NETBADB_ROUND56_PROBE").ok();
    let round58_probe = std::env::var("NETBADB_ROUND58_PROBE").ok();
    let round60_probe = std::env::var("NETBADB_ROUND60_PROBE").ok();
    let round62_probe = std::env::var("NETBADB_ROUND62_PROBE").ok();
    let round46_probe = std::env::var("NETBADB_ROUND46_PROBE").ok();
    let round46_email_not_null = round46_probe
        .as_deref()
        .is_some_and(|probe| matches!(probe, "drop" | "indexed-drop" | "drop-set"));
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
    if round60_probe.is_some() || round62_probe.is_some() {
        let nullable = round62_probe.as_deref() == Some("nullable");
        db.execute(&format!(
            "CREATE TABLE projects (id BIGINT NOT NULL, legacy TEXT{}, flag BOOLEAN)",
            if nullable { "" } else { " NOT NULL" }
        ))?;
        db.execute("CREATE INDEX projects_legacy_idx ON projects (legacy)")?;
        db.execute("INSERT INTO projects VALUES (1, '42', true)")?;
        if nullable {
            db.execute("INSERT INTO projects VALUES (2, NULL, false)")?;
            db.execute("INSERT INTO projects VALUES (3, '99', NULL)")?;
        } else {
            db.execute("INSERT INTO projects VALUES (2, '-7', false)")?;
            db.execute("INSERT INTO projects VALUES (3, 'bad', NULL)")?;
        }
    } else if round56_probe.is_some() || round58_probe.is_some() {
        db.execute("CREATE TABLE projects (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN)")?;
        if round56_probe.as_deref() == Some("indexed-drop")
            || round58_probe.as_deref() == Some("indexed-swap")
        {
            db.execute("CREATE INDEX projects_legacy_idx ON projects (legacy)")?;
        }
        db.execute("INSERT INTO projects VALUES (1, 'old1', true)")?;
        db.execute("INSERT INTO projects VALUES (2, 'old2', false)")?;
        db.execute("INSERT INTO projects VALUES (3, NULL, NULL)")?;
    } else if round42_probe.is_some()
        || round44_probe.is_some()
        || round45_probe.is_some()
        || round46_probe.is_some()
        || round48_probe.is_some()
        || round50_probe.is_some()
        || round52_probe.is_some()
        || round54_probe.is_some()
    {
        let nullability = if round46_email_not_null {
            " NOT NULL"
        } else {
            ""
        };
        db.execute(&format!(
            "CREATE TABLE projects (id BIGINT NOT NULL, legacy TEXT, email TEXT{nullability})"
        ))?;
        if round42_probe.is_some() {
            db.execute("CREATE INDEX projects_legacy_idx ON projects (legacy)")?;
        }
        db.execute("CREATE INDEX projects_email_idx ON projects (email)")?;
        let first_email = if round46_email_not_null {
            "'one@example.test'"
        } else {
            "NULL"
        };
        db.execute(&format!(
            "INSERT INTO projects VALUES (1, 'old-one', {first_email})"
        ))?;
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
    let old_index_id = db
        .indexes(TableId(2))?
        .first()
        .map_or(netbadb_types::IndexId(0), |index| index.id);
    let base_version = db
        .table_schema_version(TableId(2))
        .ok_or("projects version missing")?;
    let base_generation = db.schema_generation();
    let source_storage = db.inspect_change_stream(TableId(2))?.storage_id;
    let target_storage = db.next_storage_id().ok_or("StorageId floor missing")?;
    let round52_cursor = if let Some(probe) = &round52_probe {
        let cursor = db.enable_change_stream(TableId(2))?;
        if probe == "disabled" {
            db.disable_change_stream(TableId(2))?;
        } else if probe != "blocked" {
            return Err(format!("unknown Round 52 probe {probe}").into());
        }
        Some(cursor)
    } else {
        None
    };
    let round54_cursor = if round54_probe.as_deref() == Some("enabled") {
        Some(db.enable_change_stream(TableId(2))?)
    } else {
        None
    };
    if round62_probe.as_deref() == Some("stream") {
        db.enable_change_stream(TableId(2))?;
    }
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
    let project_columns = if round60_probe.is_some() || round62_probe.is_some() {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "legacy", "physical_type": "text", "semantic_type": null, "nullable": round62_probe.as_deref() == Some("nullable"), "primary_key": false},
            {"id": 3, "name": "flag", "physical_type": "bool", "semantic_type": null, "nullable": true, "primary_key": false}
        ])
    } else if round56_probe.is_some() || round58_probe.is_some() {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "legacy", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false},
            {"id": 3, "name": "flag", "physical_type": "bool", "semantic_type": null, "nullable": true, "primary_key": false}
        ])
    } else if round42_probe.is_some()
        || round44_probe.is_some()
        || round45_probe.is_some()
        || round46_probe.is_some()
        || round48_probe.is_some()
        || round50_probe.is_some()
        || round52_probe.is_some()
        || round54_probe.is_some()
    {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "legacy", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false},
            {"id": 3, "name": "email", "physical_type": "text", "semantic_type": null, "nullable": !round46_email_not_null, "primary_key": false}
        ])
    } else {
        json!([
            {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null, "nullable": false, "primary_key": false},
            {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
        ])
    };
    let config = serde_json::to_vec_pretty(&json!({
        "version": 8,
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
    if let Some(probe) = round62_probe {
        let winner = matches!(probe.as_str(), "autocommit" | "adopted" | "nullable");
        let expected_rows = match probe.as_str() {
            "autocommit" => Some(vec![42_i64, -7, 0]),
            "adopted" => Some(vec![43_i64, -7, 0]),
            "nullable" => None,
            "invalid" | "range" | "unsupported" | "missing" | "same" | "stream" | "rollback" => {
                None
            }
            _ => return Err(format!("unknown Round 62 probe {probe}").into()),
        };
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 62 table absent")?;
            let indexes = reopened.indexes(TableId(2))?;
            if winner {
                if projects.column("legacy").is_none_or(|column| {
                    column.id != ColumnId(4)
                        || column.semantic_type().physical != PhysicalType::Int64
                        || column.nullable != (probe == "nullable")
                }) || projects.column_by_id(ColumnId(2)).is_some()
                    || projects
                        .columns
                        .iter()
                        .any(|column| column.name.starts_with("__netbadb_alter_type_"))
                    || indexes.len() != 1
                    || indexes[0].id != netbadb_types::IndexId(old_index_id.0 + 1)
                    || indexes[0].column_id != ColumnId(4)
                    || indexes[0].name.as_ref().map(|name| name.as_str())
                        != Some("projects_legacy_idx")
                {
                    return Err("Round 62 committed schema/index mismatch".into());
                }
                let rows = reopened
                    .query("SELECT legacy FROM projects ORDER BY id")?
                    .rows;
                if probe == "nullable" {
                    if rows
                        != vec![
                            vec![ScalarValue::Int64(42)],
                            vec![ScalarValue::Null],
                            vec![ScalarValue::Int64(99)],
                        ]
                    {
                        return Err("Round 62 nullable values mismatch".into());
                    }
                } else if rows
                    != expected_rows
                        .as_ref()
                        .ok_or("Round 62 expected rows absent")?
                        .iter()
                        .map(|value| vec![ScalarValue::Int64(*value)])
                        .collect::<Vec<_>>()
                {
                    return Err("Round 62 converted values mismatch".into());
                }
            } else if projects.column("legacy").is_none_or(|column| {
                column.id != ColumnId(2) || column.semantic_type().physical != PhysicalType::Text
            }) || indexes.len() != 1
                || indexes[0].id != old_index_id
                || indexes[0].column_id != ColumnId(2)
                || reopened.next_storage_id() != Some(target_storage)
            {
                return Err("Round 62 loser changed public source authority".into());
            }
            reopened.close()?;
        }
        println!("REOPEN PASS Round 62 {probe}");
        return Ok(());
    }
    if let Some(probe) = round60_probe {
        if probe != "cast-migration" {
            return Err(format!("unknown Round 60 probe {probe}").into());
        }
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 60 converted table absent")?;
            let indexes = reopened.indexes(TableId(2))?;
            if projects.columns.len() != 3
                || projects.column("legacy").is_none_or(|column| {
                    column.id != ColumnId(4)
                        || column.nullable
                        || column.semantic_type().physical != PhysicalType::Int64
                })
                || projects.column_by_id(ColumnId(2)).is_some()
                || reopened.next_storage_id()
                    != Some(netbadb_types::StorageId(target_storage.0 + 1))
                || indexes.len() != 1
                || indexes[0].id != netbadb_types::IndexId(old_index_id.0 + 1)
                || indexes[0].column_id != ColumnId(4)
                || reopened
                    .query("SELECT id, legacy, flag FROM projects ORDER BY id")?
                    .rows
                    != vec![
                        vec![
                            ScalarValue::Int64(1),
                            ScalarValue::Int64(43),
                            ScalarValue::Bool(true),
                        ],
                        vec![
                            ScalarValue::Int64(3),
                            ScalarValue::Int64(0),
                            ScalarValue::Null,
                        ],
                        vec![
                            ScalarValue::Int64(4),
                            ScalarValue::Int64(99),
                            ScalarValue::Bool(true),
                        ],
                    ]
            {
                return Err("Round 60 production cast migration authority mismatch".into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: Round 60 production CAST migration survived three catalog-only opens; ALTER TYPE remained closed; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round58_probe {
        if probe != "indexed-swap" {
            return Err(format!("unknown Round 58 probe {probe}").into());
        }
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 58 final table absent")?;
            let indexes = reopened.indexes(TableId(2))?;
            if projects.columns.len() != 3
                || projects
                    .column("legacy")
                    .is_none_or(|column| column.id != ColumnId(4) || column.nullable)
                || projects.column_by_id(ColumnId(2)).is_some()
                || reopened.next_storage_id()
                    != Some(netbadb_types::StorageId(target_storage.0 + 1))
                || indexes.len() != 1
                || indexes[0].id != netbadb_types::IndexId(2)
                || indexes[0].column_id != ColumnId(4)
                || indexes[0].name.as_ref().map(|name| name.as_str()) != Some("projects_legacy_idx")
                || reopened
                    .query("SELECT id, legacy FROM projects ORDER BY id")?
                    .rows
                    != vec![
                        vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
                        vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                        vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
                    ]
            {
                return Err("Round 58 committed indexed shadow swap mismatch".into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: indexed-swap Round 58 result survived three catalog-only opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round56_probe {
        let winner = matches!(probe.as_str(), "commit" | "rename-table");
        if !winner && !matches!(probe.as_str(), "rollback" | "indexed-drop") {
            return Err(format!("unknown Round 56 probe {probe}").into());
        }
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let final_name = if probe == "rename-table" {
                "people"
            } else {
                "projects"
            };
            let projects = reopened
                .schema()
                .table(final_name)
                .ok_or("Round 56 final table absent")?;
            if winner {
                if projects.columns.len() != 3
                    || projects
                        .column("legacy")
                        .is_none_or(|column| column.id != ColumnId(4) || column.nullable)
                    || projects.column_by_id(ColumnId(2)).is_some()
                    || reopened.next_storage_id()
                        != Some(netbadb_types::StorageId(target_storage.0 + 1))
                    || reopened.indexes(TableId(2))?.iter().all(|index| {
                        index.column_id != ColumnId(4)
                            || index.name.as_ref().map(|name| name.as_str())
                                != Some("projects_legacy_idx")
                    })
                    || reopened
                        .query(&format!("SELECT id, legacy FROM {final_name} ORDER BY id"))?
                        .rows
                        != vec![
                            vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
                            vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                            vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
                        ]
                {
                    return Err("Round 56 committed shadow swap mismatch".into());
                }
            } else if projects.column("shadow").is_some()
                || projects
                    .column("legacy")
                    .is_none_or(|column| column.id != ColumnId(2))
                || reopened.next_storage_id() != Some(target_storage)
            {
                return Err("Round 56 rollback/failed terminal operation changed the base".into());
            }
            reopened.close()?;
        }
        println!("REOPEN PASS Round 56 {probe}");
        return Ok(());
    }
    if let Some(probe) = round52_probe {
        let cursor = round52_cursor.ok_or("Round 52 cursor absent")?;
        for _ in 0..3 {
            let reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 52 table absent")?;
            let stream = reopened.inspect_change_stream(TableId(2))?;
            if probe == "blocked" {
                if projects.column("marker").is_some()
                    || reopened.next_storage_id() != Some(target_storage)
                    || stream.storage_id != cursor.storage_id
                    || stream.status != netbadb_storage::ChangeStreamStatus::Enabled
                    || stream.generation != Some(cursor.generation)
                    || stream.current_data_version != cursor.frontier
                {
                    return Err("Round 52 blocked replacement mismatch".into());
                }
            } else if projects.column("marker").is_none()
                || reopened.next_storage_id()
                    != Some(netbadb_types::StorageId(target_storage.0 + 1))
                || stream.storage_id != target_storage
                || stream.status != netbadb_storage::ChangeStreamStatus::Disabled
            {
                return Err("Round 52 disabled-stream winner mismatch".into());
            }
            reopened.close()?;
        }
        if probe == "disabled" {
            let mut reopened = Database::open_catalog(&catalog)?;
            let new_cursor = reopened.enable_change_stream(TableId(2))?;
            let anchor = reopened.committed_read_anchor(TableId(2))?;
            if new_cursor.storage_id != target_storage || anchor.cursor != new_cursor {
                return Err("Round 52 S2 rebaseline mismatch".into());
            }
            reopened.close()?;
        }
        println!("REOPEN PASS Round 52 {probe}");
        return Ok(());
    }
    if let Some(probe) = round54_probe {
        let winner = probe == "commit";
        if !winner && !matches!(probe.as_str(), "rollback" | "enabled") {
            return Err(format!("unknown Round 54 probe {probe}").into());
        }
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 54 table absent")?;
            if winner {
                if projects
                    .column("marker")
                    .is_none_or(|column| column.id != ColumnId(4) || column.nullable)
                    || projects
                        .column("normalized")
                        .is_none_or(|column| column.id != ColumnId(5) || column.nullable)
                    || reopened.next_storage_id()
                        != Some(netbadb_types::StorageId(target_storage.0 + 1))
                    || reopened.indexes(TableId(2))?.iter().all(|index| {
                        index
                            .name
                            .as_ref()
                            .is_none_or(|name| name.as_str() != "projects_normalized_idx")
                    })
                    || reopened
                        .query("SELECT id, marker, normalized FROM projects ORDER BY id")?
                        .rows
                        != vec![
                            vec![
                                ScalarValue::Int64(1),
                                ScalarValue::Text("updated1".into()),
                                ScalarValue::Text("updated1".into()),
                            ],
                            vec![
                                ScalarValue::Int64(3),
                                ScalarValue::Text("missing".into()),
                                ScalarValue::Text("missing".into()),
                            ],
                            vec![
                                ScalarValue::Int64(4),
                                ScalarValue::Text("inserted4".into()),
                                ScalarValue::Text("inserted4".into()),
                            ],
                        ]
                {
                    return Err("Round 54 committed VirtualRow result mismatch".into());
                }
            } else if projects.column("marker").is_some()
                || reopened.inspect_change_stream(TableId(2))?.storage_id
                    != round54_cursor.map_or(source_storage, |cursor| cursor.storage_id)
                || reopened.next_storage_id() != Some(target_storage)
            {
                return Err("Round 54 rollback/blocked result mismatch".into());
            }
            if let Some(cursor) = round54_cursor {
                let stream = reopened.inspect_change_stream(TableId(2))?;
                if stream.status != netbadb_storage::ChangeStreamStatus::Enabled
                    || stream.current_data_version != cursor.frontier
                    || stream.committed_batch_count != 0
                {
                    return Err("Round 54 enabled-stream loser emitted a change batch".into());
                }
            }
            reopened.close()?;
        }
        println!("REOPEN PASS Round 54 {probe}");
        return Ok(());
    }
    if let Some(probe) = round50_probe {
        let winner = matches!(probe.as_str(), "commit" | "commit-bound");
        if !winner && probe != "rollback" {
            return Err(format!("unknown Round 50 probe {probe}").into());
        }
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 50 table absent")?;
            if winner {
                if projects
                    .column("marker")
                    .is_none_or(|column| column.id != ColumnId(4) || column.nullable)
                    || reopened.table_schema_version(TableId(2))
                        != Some(netbadb_types::TableSchemaVersion(base_version.0 + 1))
                    || reopened.schema_generation().0 != base_generation.0 + 1
                    || reopened.next_storage_id()
                        != Some(netbadb_types::StorageId(target_storage.0 + 1))
                    || reopened
                        .query("SELECT id, marker FROM projects ORDER BY id")?
                        .rows
                        != vec![
                            vec![
                                ScalarValue::Int64(1),
                                ScalarValue::Text(if probe == "commit-bound" {
                                    "bound-value".into()
                                } else {
                                    "updated1".into()
                                }),
                            ],
                            vec![ScalarValue::Int64(3), ScalarValue::Text("old-three".into())],
                            vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
                        ]
                    || !reopened.indexes(TableId(2))?.iter().any(|index| {
                        index
                            .name
                            .as_ref()
                            .is_some_and(|name| name.as_str() == "projects_marker_idx")
                    })
                {
                    return Err("Round 50 winner mismatch".into());
                }
            } else if projects.column("marker").is_some()
                || reopened.table_schema_version(TableId(2)) != Some(base_version)
                || reopened.schema_generation() != base_generation
                || reopened.next_storage_id() != Some(target_storage)
                || reopened.query("SELECT id FROM projects ORDER BY id")?.rows
                    != vec![
                        vec![ScalarValue::Int64(1)],
                        vec![ScalarValue::Int64(2)],
                        vec![ScalarValue::Int64(3)],
                    ]
            {
                return Err("Round 50 rollback mismatch".into());
            }
            reopened.close()?;
        }
        println!("REOPEN PASS Round 50 {probe}");
        return Ok(());
    }
    if let Some(probe) = round48_probe {
        let dirty = matches!(probe.as_str(), "cnew" | "rename");
        let rollback = probe == "rollback";
        let names: &[&str] = match probe.as_str() {
            "cnew" => &["projects_email_idx", "projects_marker_idx"],
            "rename" => &["projects_contact_idx"],
            "create" => &["projects_email_idx", "projects_legacy_idx"],
            "drop" => &[],
            "create-drop" | "rollback" | "replacement" => &["projects_email_idx"],
            "multiple" => &["projects_legacy_idx", "projects_id_idx"],
            _ => return Err(format!("unknown Round 48 probe {probe}").into()),
        };
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 48 table absent")?;
            if projects.column("marker").is_some() != (probe == "cnew")
                || projects
                    .column(if probe == "rename" {
                        "contact"
                    } else {
                        "email"
                    })
                    .map(|column| column.id)
                    != Some(ColumnId(3))
                || reopened.table_schema_version(TableId(2))
                    != Some(netbadb_types::TableSchemaVersion(
                        base_version.0 + u64::from(dirty),
                    ))
                || reopened.schema_generation().0 != base_generation.0 + u64::from(dirty)
                || reopened.next_storage_id()
                    != Some(netbadb_types::StorageId(
                        target_storage.0 + u64::from(dirty),
                    ))
            {
                return Err(format!("Round 48 schema/placement mismatch: {probe}").into());
            }
            let indexes = reopened.indexes(TableId(2))?;
            if indexes.len() != names.len()
                || names.iter().any(|name| {
                    !indexes.iter().any(|index| {
                        index
                            .name
                            .as_ref()
                            .is_some_and(|candidate| candidate.as_str() == *name)
                    })
                })
                || (probe == "replacement" && indexes[0].id == old_index_id)
            {
                return Err(format!("Round 48 final index inventory mismatch: {probe}").into());
            }
            let expected = if rollback {
                vec![(1, "old-one"), (2, "old-two"), (3, "old-three")]
            } else {
                vec![(1, "updated1"), (3, "old-three"), (4, "inserted4")]
            };
            let rows = reopened
                .query("SELECT id, legacy FROM projects ORDER BY id")?
                .rows;
            let expected: Vec<Vec<netbadb_types::ScalarValue>> = expected
                .into_iter()
                .map(|(id, value)| {
                    vec![
                        netbadb_types::ScalarValue::Int64(id),
                        netbadb_types::ScalarValue::Text(value.into()),
                    ]
                })
                .collect();
            if rows != expected {
                return Err(format!("Round 48 visible rows mismatch: {probe}").into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 48 final schema/index identity and visible rows verified across three opens; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round46_probe {
        for _ in 0..3 {
            let reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 46 table disappeared")?;
            let indexes = reopened.indexes(TableId(2))?;
            let effective_set = matches!(
                probe.as_str(),
                "set"
                    | "indexed-set"
                    | "own-update"
                    | "own-delete"
                    | "rename-set"
                    | "add-set"
                    | "extended-set"
            );
            let effective_drop = matches!(probe.as_str(), "drop" | "indexed-drop");
            let no_op = matches!(probe.as_str(), "set-drop" | "drop-set");
            let failure = matches!(
                probe.as_str(),
                "set-failure"
                    | "own-insert-null"
                    | "zero-row"
                    | "cnew-set"
                    | "cnew-drop"
                    | "post-refinement-dml"
            );
            if !(effective_set || effective_drop || no_op || failure) {
                return Err(format!("unknown Round 46 probe {probe}").into());
            }
            let email_name = if probe == "rename-set" {
                "contact"
            } else {
                "email"
            };
            let expected_nullable = if effective_set {
                false
            } else if effective_drop {
                true
            } else {
                probe != "drop-set"
            };
            let expected_effective = effective_set || effective_drop;
            if projects
                .column(email_name)
                .map(|column| (column.id, column.nullable))
                != Some((ColumnId(3), expected_nullable))
                || indexes.len() != 1
                || indexes[0].id != old_index_id
                || indexes[0].column_id != ColumnId(3)
                || reopened.table_schema_version(TableId(2))
                    != Some(if expected_effective {
                        netbadb_types::TableSchemaVersion(base_version.0 + 1)
                    } else {
                        base_version
                    })
                || reopened.schema_generation()
                    != if expected_effective {
                        netbadb_types::SchemaGeneration(base_generation.0 + 1)
                    } else {
                        base_generation
                    }
                || reopened.next_storage_id()
                    != Some(if expected_effective {
                        netbadb_types::StorageId(target_storage.0 + 1)
                    } else {
                        target_storage
                    })
                || (probe == "add-set" && projects.column("marker").is_none())
            {
                return Err(format!("Round 46 probe {probe} reopened incorrectly").into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 46 surviving-base nullability result survived three catalog-only opens; final index spec validated; manifest unchanged"
        );
        return Ok(());
    }
    if let Some(probe) = round45_probe {
        for _ in 0..3 {
            let reopened = Database::open_catalog(&catalog)?;
            let projects = reopened
                .schema()
                .table("projects")
                .ok_or("Round 45 baseline table disappeared")?;
            let indexes = reopened.indexes(TableId(2))?;
            let known_probe = matches!(
                probe.as_str(),
                "set-not-null"
                    | "drop-not-null"
                    | "create-index"
                    | "select-after"
                    | "insert-after"
                    | "update-after"
                    | "delete-after"
            );
            if !known_probe
                || projects.columns.len() != 3
                || projects.column("legacy").map(|column| column.id) != Some(ColumnId(2))
                || projects
                    .column("email")
                    .map(|column| (column.id, column.nullable))
                    != Some((ColumnId(3), true))
                || reopened.table_schema_version(TableId(2)) != Some(base_version)
                || reopened.schema_generation() != base_generation
                || reopened.next_storage_id() != Some(target_storage)
                || indexes.len() != 1
                || indexes[0].id != old_index_id
            {
                return Err(format!("Round 45 negative probe {probe} changed the base").into());
            }
            reopened.close()?;
        }
        println!(
            "REOPEN PASS: {probe} Round 45 unsupported candidate left the base intact across three catalog-only opens; manifest unchanged"
        );
        return Ok(());
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
