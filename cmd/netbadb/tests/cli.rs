use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_sdk::{
    ColumnDef, ColumnId, Database, PhysicalType, ScalarValue, TableDef, TableId, TypeSpec,
};
use serde_json::{Value, json};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    directory: PathBuf,
    manifest: PathBuf,
    users_path: PathBuf,
    admin_path: PathBuf,
    users: TableDef,
    admin: TableDef,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "netbadb-cli-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let users_path = directory.join("users.ndb");
        let admin_path = directory.join("admin.ndb");
        let users = TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                    .primary_key(true),
                ColumnDef::new(
                    ColumnId(2),
                    "team_id",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(4),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        );
        let admin = TableDef::new(
            TableId(2),
            "admin_data",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        );
        let mut database = Database::create_tables(vec![
            (users_path.clone(), users.clone()),
            (admin_path.clone(), admin.clone()),
        ])
        .unwrap();
        for id in 0..80_i64 {
            database
                .insert_into(
                    users.id,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Int64(id % 2),
                        ScalarValue::Text(format!("member-{id:03}-{}", "x".repeat(500))),
                        ScalarValue::Bool(id % 3 == 0),
                    ],
                )
                .unwrap();
        }
        database
            .insert_into(admin.id, &[ScalarValue::Int64(7)])
            .unwrap();
        database.create_index(users.id, ColumnId(2)).unwrap();
        database.create_index(users.id, ColumnId(1)).unwrap();
        database.analyze(users.id).unwrap();
        database.close().unwrap();

        let manifest = directory.join("server.json");
        write_manifest(&manifest, 4, "users");
        Self {
            directory,
            manifest,
            users_path,
            admin_path,
            users,
            admin,
        }
    }

    fn tables(&self) -> Vec<(PathBuf, TableDef)> {
        vec![
            (self.users_path.clone(), self.users.clone()),
            (self.admin_path.clone(), self.admin.clone()),
        ]
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn write_manifest(path: &Path, version: u32, users_table_name: &str) {
    let manifest = json!({
        "version": version,
        "authorization": {
            "local_plaintext": {
                "tables": [{
                    "table_id": 1,
                    "read": true,
                    "write": true,
                    "transaction": true,
                    "analyze": true
                }]
            },
            "clients": []
        },
        "tables": [
            {
                "path": "users.ndb",
                "id": 1,
                "name": users_table_name,
                "columns": [
                    {
                        "id": 1,
                        "name": "id",
                        "physical_type": "int64",
                        "semantic_type": null,
                        "nullable": false,
                        "primary_key": true
                    },
                    {
                        "id": 2,
                        "name": "team_id",
                        "physical_type": "int64",
                        "semantic_type": null,
                        "nullable": false,
                        "primary_key": false
                    },
                    {
                        "id": 3,
                        "name": "name",
                        "physical_type": "text",
                        "semantic_type": null,
                        "nullable": false,
                        "primary_key": false
                    },
                    {
                        "id": 4,
                        "name": "active",
                        "physical_type": "bool",
                        "semantic_type": null,
                        "nullable": false,
                        "primary_key": false
                    }
                ]
            },
            {
                "path": "admin.ndb",
                "id": 2,
                "name": "admin_data",
                "columns": [{
                    "id": 1,
                    "name": "id",
                    "physical_type": "int64",
                    "semantic_type": null,
                    "nullable": false,
                    "primary_key": false
                }]
            }
        ]
    });
    std::fs::write(path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
}

fn netbadb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_netbadb"))
}

fn catalog(fixture: &Fixture, format: &str) -> Output {
    netbadb()
        .args(["inspect", "catalog", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--format", format])
        .output()
        .unwrap()
}

fn statement(fixture: &Fixture, sql: &str, format: &str) -> Output {
    netbadb()
        .args(["inspect", "statement", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--sql", sql, "--format", format])
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

fn json_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn collect_operators<'a>(value: &'a Value, operators: &mut Vec<&'a str>) {
    match value {
        Value::Object(fields) => {
            if let Some(operator) = fields.get("operator").and_then(Value::as_str) {
                operators.push(operator);
            }
            for value in fields.values() {
                collect_operators(value, operators);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_operators(value, operators);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn collect_scans(value: &Value, scans: &mut Vec<(u64, u64)>) {
    match value {
        Value::Object(fields) => {
            if matches!(
                fields.get("operator").and_then(Value::as_str),
                Some("seq_scan" | "index_scan")
            ) {
                scans.push((
                    fields["table_id"].as_u64().unwrap(),
                    fields["binding_id"].as_u64().unwrap(),
                ));
            }
            for value in fields.values() {
                collect_scans(value, scans);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_scans(value, scans);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[test]
fn catalog_text_and_json_are_complete_deterministic_and_ignore_network_acl_filtering() {
    let fixture = Fixture::new("catalog");
    let text = catalog(&fixture, "text");
    assert!(text.status.success());
    assert!(text.stderr.is_empty());
    let text = stdout(&text);
    assert!(text.contains("Table users #1"));
    assert!(text.contains("Table admin_data #2"));
    assert!(text.contains("[0] column #2 team_id"));
    assert!(text.contains("[1] column #1 id"));

    let first = catalog(&fixture, "json");
    let second = catalog(&fixture, "json");
    assert!(first.status.success());
    assert!(first.stderr.is_empty());
    assert_eq!(first.stdout, second.stdout);
    let json = json_stdout(&first);
    assert_eq!(json["format"], "netbadb-inspection");
    assert_eq!(json["version"], 3);
    assert_eq!(json["kind"], "catalog");
    assert_eq!(json["catalog"]["tables"][0]["name"], "users");
    assert_eq!(json["catalog"]["tables"][1]["name"], "admin_data");
    let output = stdout(&first);
    assert!(!output.contains(fixture.directory.to_string_lossy().as_ref()));
    assert!(!output.contains("authorization"));
    assert!(!output.contains("certificate"));
    assert!(!output.contains("listen"));
}

#[test]
fn statement_commands_report_real_plans_sql_files_bindings_and_aggregate_provenance() {
    let fixture = Fixture::new("statement");
    let indexed_sql = "SELECT name FROM users WHERE id = 42";
    let indexed_text = statement(&fixture, indexed_sql, "text");
    assert!(indexed_text.status.success(), "{}", stderr(&indexed_text));
    assert!(stdout(&indexed_text).contains("IndexScan"));
    assert!(indexed_text.stderr.is_empty());

    let indexed = statement(&fixture, indexed_sql, "json");
    let indexed_json = json_stdout(&indexed);
    let mut operators = Vec::new();
    collect_operators(&indexed_json, &mut operators);
    assert!(operators.contains(&"filter"));
    assert!(operators.contains(&"index_scan"));

    let ranged_sql = "SELECT id FROM users WHERE id >= 40 AND id < 45";
    let ranged_text = statement(&fixture, ranged_sql, "text");
    assert!(ranged_text.status.success(), "{}", stderr(&ranged_text));
    assert!(stdout(&ranged_text).contains("RangeIndexScan"));
    let ranged = json_stdout(&statement(&fixture, ranged_sql, "json"));
    operators.clear();
    collect_operators(&ranged, &mut operators);
    assert!(operators.contains(&"range_index_scan"));
    assert_eq!(ranged["version"], 3);
    let range = &ranged["statement"]["plan"]["root"]["input"]["input"];
    assert_eq!(range["lower_bound"]["kind"], "included");
    assert_eq!(range["lower_bound"]["value"]["value"], 40);
    assert_eq!(range["upper_bound"]["kind"], "excluded");
    assert_eq!(range["upper_bound"]["value"]["value"], 45);

    let duplicate_heavy = statement(&fixture, "SELECT name FROM users WHERE team_id = 0", "json");
    let duplicate_json = json_stdout(&duplicate_heavy);
    operators.clear();
    collect_operators(&duplicate_json, &mut operators);
    assert!(operators.contains(&"seq_scan"));
    assert!(!operators.contains(&"index_scan"));

    let sql_file = fixture.directory.join("query.sql");
    std::fs::write(&sql_file, "SELECT\n  name\nFROM users\nWHERE id = 42;\n").unwrap();
    let file_output = netbadb()
        .args(["inspect", "statement", "--manifest"])
        .arg(&fixture.manifest)
        .arg("--sql-file")
        .arg(&sql_file)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert!(file_output.status.success());
    assert_eq!(file_output.stdout, indexed.stdout);

    let self_join = statement(
        &fixture,
        "SELECT e.id, m.id FROM users e JOIN users m ON e.team_id = m.team_id",
        "json",
    );
    let self_join_json = json_stdout(&self_join);
    let mut scans = Vec::new();
    collect_scans(&self_join_json, &mut scans);
    assert_eq!(scans, vec![(1, 0), (1, 1)]);
    let hash_join = &self_join_json["statement"]["plan"]["root"]["input"];
    assert_eq!(hash_join["operator"], "hash_join");
    assert_eq!(hash_join["left_key"]["binding_id"], 0);
    assert_eq!(hash_join["right_key"]["binding_id"], 1);
    assert!(hash_join.get("build_side").is_none());

    let non_equi = json_stdout(&statement(
        &fixture,
        "SELECT e.id, m.id FROM users e JOIN users m ON e.id < m.id",
        "json",
    ));
    assert_eq!(
        non_equi["statement"]["plan"]["root"]["input"]["operator"],
        "nested_loop_join"
    );

    let aggregate = statement(
        &fixture,
        "SELECT team_id, COUNT(*) FROM users GROUP BY team_id",
        "json",
    );
    let aggregate = json_stdout(&aggregate);
    operators.clear();
    collect_operators(&aggregate, &mut operators);
    assert!(operators.contains(&"aggregate"));
    assert!(aggregate["statement"]["result"]["columns"][1]["source"].is_null());
    assert_eq!(
        aggregate["statement"]["plan"]["root"]["outputs"][1]["kind"],
        "aggregate"
    );
}

#[test]
fn dml_is_never_executed_and_failures_leave_stdout_empty_with_coarse_exit_codes() {
    let fixture = Fixture::new("dml-errors");
    let delete = statement(&fixture, "DELETE FROM users", "json");
    assert!(delete.status.success());
    assert_eq!(json_stdout(&delete)["statement"]["kind"], "delete");

    let mut database = Database::open_tables(fixture.tables()).unwrap();
    assert_eq!(
        database.query("SELECT id FROM users").unwrap().rows.len(),
        80
    );
    database.close().unwrap();

    let invalid = statement(&fixture, "SELECT FROM", "json");
    assert_eq!(invalid.status.code(), Some(1));
    assert!(invalid.stdout.is_empty());
    assert!(stderr(&invalid).contains("inspection failed"));

    let missing_manifest = netbadb()
        .args(["inspect", "catalog", "--format", "json"])
        .output()
        .unwrap();
    assert_eq!(missing_manifest.status.code(), Some(2));
    assert!(missing_manifest.stdout.is_empty());

    let both_sources = netbadb()
        .args(["inspect", "statement", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--sql", "SELECT id FROM users", "--sql-file", "query.sql"])
        .output()
        .unwrap();
    assert_eq!(both_sources.status.code(), Some(2));
    assert!(both_sources.stdout.is_empty());

    let unknown_format = netbadb()
        .args(["inspect", "catalog", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--format", "yaml"])
        .output()
        .unwrap();
    assert_eq!(unknown_format.status.code(), Some(2));
    assert!(unknown_format.stdout.is_empty());
}

#[test]
fn manifest_and_input_failures_precede_output_and_schema_mismatch_is_rejected() {
    let fixture = Fixture::new("manifest-errors");
    let version_three = fixture.directory.join("v3.json");
    write_manifest(&version_three, 3, "users");
    let old_manifest = netbadb()
        .args(["inspect", "catalog", "--manifest"])
        .arg(&version_three)
        .output()
        .unwrap();
    assert_eq!(old_manifest.status.code(), Some(1));
    assert!(old_manifest.stdout.is_empty());
    assert!(stderr(&old_manifest).contains("unsupported deployment manifest version 3"));

    let mismatch = fixture.directory.join("mismatch.json");
    write_manifest(&mismatch, 4, "other_users");
    let mismatch = netbadb()
        .args(["inspect", "catalog", "--manifest"])
        .arg(&mismatch)
        .output()
        .unwrap();
    assert_eq!(mismatch.status.code(), Some(1));
    assert!(mismatch.stdout.is_empty());
    assert!(stderr(&mismatch).contains("schema fingerprint"));

    let missing_sql = fixture.directory.join("missing.sql");
    let missing_input = netbadb()
        .args([
            "inspect",
            "statement",
            "--manifest",
            "missing-manifest.json",
        ])
        .arg("--sql-file")
        .arg(&missing_sql)
        .output()
        .unwrap();
    assert_eq!(missing_input.status.code(), Some(1));
    assert!(missing_input.stdout.is_empty());
    assert!(stderr(&missing_input).contains("failed to read SQL file"));
}
