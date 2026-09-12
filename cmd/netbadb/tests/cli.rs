use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_client::{Client, Config as ClientConfig};
use netbadb_sdk::{
    ColumnDef, ColumnId, Database, DatabaseCoordinatorConfig, PhysicalType, ScalarValue, TableDef,
    TableId, TableStorageCreateSpec, TypeSpec,
};
use netbadb_server::{ServerConfig, TcpServer};
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
        write_manifest(&manifest, 8, "users");
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
fn inspect_accepts_and_validates_v8_adaptive_without_rewriting_the_manifest() {
    let fixture = Fixture::new("adaptive-manifest");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&fixture.manifest).unwrap()).unwrap();
    manifest["adaptive"] = json!({
        "mode": "feedback_only",
        "feedback": {
            "limits": {
                "max_target_windows": 2,
                "workload": {
                    "max_query_shapes": 3,
                    "max_plan_variants_per_shape": 4
                },
                "max_calibration_epochs": 5,
                "max_calibration_query_shapes": 6,
                "max_calibration_plan_variants_per_shape": 7
            }
        }
    });
    manifest["physical_design"] = json!({
        "evidence_limits": {
            "max_index_candidates": 4,
            "max_columnar_candidates": 4,
            "max_query_shapes_per_candidate": 4,
            "max_columnar_columns_per_candidate": 4
        },
        "advisor_policy": {
            "index": {
                "minimum_reports": 1,
                "minimum_distinct_query_shapes": 1,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 4
            },
            "columnar": {
                "minimum_reports": 1,
                "minimum_distinct_query_shapes": 1,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 4
            }
        }
    });
    let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    std::fs::write(&fixture.manifest, &bytes).unwrap();

    let inspected = catalog(&fixture, "json");
    assert!(inspected.status.success(), "{}", stderr(&inspected));
    assert_eq!(std::fs::read(&fixture.manifest).unwrap(), bytes);

    let document = include_str!("../../../docs/server-manifest-v8.md");
    let documented: Value = serde_json::from_str(
        document
            .split_once("```json\n")
            .and_then(|(_, remainder)| remainder.split_once("\n```"))
            .map(|(example, _)| example)
            .expect("v8 documentation contains a JSON example"),
    )
    .unwrap();
    manifest["adaptive"] = documented["adaptive"].clone();
    let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    std::fs::write(&fixture.manifest, &bytes).unwrap();
    let driven = catalog(&fixture, "json");
    assert!(driven.status.success(), "{}", stderr(&driven));
    assert_eq!(std::fs::read(&fixture.manifest).unwrap(), bytes);

    manifest["adaptive"]["feedback"]["limits"]["magic"] = json!(true);
    std::fs::write(
        &fixture.manifest,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let invalid = catalog(&fixture, "json");
    assert_eq!(invalid.status.code(), Some(1));
    assert!(invalid.stdout.is_empty());
    assert!(stderr(&invalid).contains("invalid deployment manifest JSON"));
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
    let version_five = fixture.directory.join("v5.json");
    write_manifest(&version_five, 5, "users");
    let old_manifest = netbadb()
        .args(["inspect", "catalog", "--manifest"])
        .arg(&version_five)
        .output()
        .unwrap();
    assert_eq!(old_manifest.status.code(), Some(1));
    assert!(old_manifest.stdout.is_empty());
    assert!(stderr(&old_manifest).contains("unsupported deployment manifest version 5"));

    let mismatch = fixture.directory.join("mismatch.json");
    write_manifest(&mismatch, 8, "other_users");
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

#[cfg(unix)]
#[test]
fn operator_cli_uses_live_nbop_and_never_infers_rotation_epoch() {
    let fixture = Fixture::new("operator-live");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&fixture.manifest).unwrap()).unwrap();
    manifest["listen"] = json!("127.0.0.1:0");
    manifest["adaptive"] = json!({
        "mode": "feedback_only",
        "feedback": {
            "limits": {
                "max_target_windows": 4,
                "workload": {
                    "max_query_shapes": 4,
                    "max_plan_variants_per_shape": 4
                },
                "max_calibration_epochs": 4,
                "max_calibration_query_shapes": 4,
                "max_calibration_plan_variants_per_shape": 4
            }
        }
    });
    manifest["physical_design"] = json!({
        "evidence_limits": {
            "max_index_candidates": 4,
            "max_columnar_candidates": 4,
            "max_query_shapes_per_candidate": 4,
            "max_columnar_columns_per_candidate": 4
        },
        "advisor_policy": {
            "index": {
                "minimum_reports": 1,
                "minimum_distinct_query_shapes": 1,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 4
            },
            "columnar": {
                "minimum_reports": 1,
                "minimum_distinct_query_shapes": 1,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 4
            }
        }
    });
    let socket = PathBuf::from(format!("/tmp/netbadb-cli-op-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);
    manifest["operator"] = json!({
        "unix_socket": socket,
        "io_timeout_ms": 1000,
        "allow_physical_index_apply": false
    });
    std::fs::write(
        &fixture.manifest,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let server = TcpServer::new(ServerConfig::from_manifest_path(&fixture.manifest).unwrap())
        .start()
        .unwrap();

    let status = netbadb()
        .args(["operator", "status", "--manifest"])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert!(status.status.success(), "{}", stderr(&status));
    assert!(stdout(&status).contains("Adaptive: feedback-only"));
    assert!(stdout(&status).contains("adaptive evidence window epoch: 0"));
    assert!(stdout(&status).contains("Physical Design: enabled"));
    assert!(stdout(&status).contains("Physical index apply: disabled"));
    assert!(stdout(&status).contains("Runtime token: none"));
    assert!(stdout(&status).contains("design evidence epoch: 0"));

    let missing_epoch = netbadb()
        .args(["operator", "rotate-evidence", "--manifest"])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(missing_epoch.status.code(), Some(2));
    assert!(stderr(&missing_epoch).contains("--expected-window-epoch is required"));

    let rotated = netbadb()
        .args(["operator", "rotate-evidence", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--expected-window-epoch", "0"])
        .output()
        .unwrap();
    assert!(rotated.status.success(), "{}", stderr(&rotated));
    assert!(stdout(&rotated).contains("previous epoch 0, new epoch 1"));

    let stale = netbadb()
        .args(["operator", "rotate-evidence", "--manifest"])
        .arg(&fixture.manifest)
        .args(["--expected-window-epoch", "0"])
        .output()
        .unwrap();
    assert_eq!(stale.status.code(), Some(1));
    assert!(stderr(&stale).contains("evidence window changed"));

    let reset = netbadb()
        .args(["operator", "reset-faulted-scheduler", "--manifest"])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(reset.status.code(), Some(1));
    assert!(stderr(&reset).contains("adaptive driver is not enabled"));

    let no_evidence = netbadb()
        .args([
            "operator",
            "physical-design",
            "recommendations",
            "--manifest",
        ])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(no_evidence.status.code(), Some(1));
    assert!(stderr(&no_evidence).contains("physical-design evidence window is empty"));

    let apply_disabled = netbadb()
        .args(["operator", "physical-design", "apply-index", "--manifest"])
        .arg(&fixture.manifest)
        .args([
            "--expected-runtime-token",
            "00112233445566778899aabbccddeeff",
            "--expected-evidence-epoch",
            "0",
            "--table-id",
            "1",
            "--column-id",
            "3",
            "--index-name",
            "idx_users_name",
        ])
        .output()
        .unwrap();
    assert_eq!(apply_disabled.status.code(), Some(1));
    assert!(stderr(&apply_disabled).contains("not enabled by the manifest"));

    let missing_design_epoch = netbadb()
        .args([
            "operator",
            "physical-design",
            "rotate-evidence",
            "--manifest",
        ])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(missing_design_epoch.status.code(), Some(2));
    assert!(stderr(&missing_design_epoch).contains("--expected-evidence-epoch is required"));

    let design_rotated = netbadb()
        .args([
            "operator",
            "physical-design",
            "rotate-evidence",
            "--manifest",
        ])
        .arg(&fixture.manifest)
        .args(["--expected-evidence-epoch", "0"])
        .output()
        .unwrap();
    assert!(
        design_rotated.status.success(),
        "{}",
        stderr(&design_rotated)
    );
    assert!(stdout(&design_rotated).contains("previous epoch 0, new epoch 1"));

    server.shutdown().unwrap();
    assert!(!socket.exists());
}

#[cfg(unix)]
#[test]
fn operator_cli_explicitly_applies_and_exactly_retries_a_current_recommendation() {
    let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "netbadb-cli-operator-apply-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir(&directory).unwrap();
    let users = TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create_catalog(
        directory.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            directory.join("users.ndb"),
            users,
        )],
        Some(
            DatabaseCoordinatorConfig::new(directory.join("coordinator")).with_global_visibility(),
        ),
    )
    .unwrap();
    database
        .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();
    database.close().unwrap();

    let socket = PathBuf::from(format!(
        "/tmp/netbadb-cli-apply-{}-{sequence}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket);
    let manifest_path = directory.join("server.json");
    let manifest = json!({
        "version": 8,
        "listen": "127.0.0.1:0",
        "authorization": {
            "local_plaintext": {
                "tables": [{
                    "table_id": 1,
                    "read": true,
                    "write": true,
                    "transaction": true,
                    "analyze": false
                }]
            },
            "clients": []
        },
        "tables": [{
            "path": "users.ndb",
            "id": 1,
            "name": "users",
            "columns": [
                {"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":true},
                {"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":false,"primary_key":false}
            ]
        }],
        "physical_design": {
            "evidence_limits": {
                "max_index_candidates": 8,
                "max_columnar_candidates": 8,
                "max_query_shapes_per_candidate": 8,
                "max_columnar_columns_per_candidate": 8
            },
            "advisor_policy": {
                "index": {
                    "minimum_reports": 1,
                    "minimum_distinct_query_shapes": 1,
                    "minimum_actual_scan_work_units": 0,
                    "max_recommendations": 8
                },
                "columnar": {
                    "minimum_reports": 1,
                    "minimum_distinct_query_shapes": 1,
                    "minimum_actual_scan_work_units": 0,
                    "max_recommendations": 8
                }
            }
        },
        "operator": {
            "unix_socket": socket,
            "io_timeout_ms": 1000,
            "allow_physical_index_apply": true
        }
    });
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let server = TcpServer::new(ServerConfig::from_manifest_path(&manifest_path).unwrap())
        .start()
        .unwrap();
    let mut client = Client::connect(ClientConfig::new(server.local_addr().to_string())).unwrap();
    client
        .query("SELECT id FROM users WHERE name = 'Ada'")
        .unwrap()
        .close()
        .unwrap();

    let recommendations = netbadb()
        .args([
            "operator",
            "physical-design",
            "recommendations",
            "--manifest",
        ])
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(
        recommendations.status.success(),
        "{}",
        stderr(&recommendations)
    );
    let recommendations = stdout(&recommendations);
    let token = recommendations
        .lines()
        .find_map(|line| line.strip_prefix("Runtime token: "))
        .unwrap();
    let epoch = recommendations
        .lines()
        .find_map(|line| line.strip_prefix("Evidence epoch: "))
        .unwrap();

    let apply = |token: &str, epoch: &str| {
        netbadb()
            .args(["operator", "physical-design", "apply-index", "--manifest"])
            .arg(&manifest_path)
            .args([
                "--expected-runtime-token",
                token,
                "--expected-evidence-epoch",
                epoch,
                "--table-id",
                "1",
                "--column-id",
                "2",
                "--index-name",
                "users_name_cli_idx",
            ])
            .output()
            .unwrap()
    };
    let created = apply(token, epoch);
    assert!(created.status.success(), "{}", stderr(&created));
    assert!(stdout(&created).contains("created"));
    let retried = apply(token, epoch);
    assert!(retried.status.success(), "{}", stderr(&retried));
    assert!(stdout(&retried).contains("already applied"));

    client
        .query("SELECT id FROM users WHERE name = 'Ada'")
        .unwrap()
        .close()
        .unwrap();
    drop(client);
    server.shutdown().unwrap();
    assert!(!socket.exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn operator_cli_reports_unconfigured_and_offline_planes_without_opening_database() {
    let fixture = Fixture::new("operator-errors");
    let unconfigured = netbadb()
        .args(["operator", "status", "--manifest"])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(unconfigured.status.code(), Some(1));
    assert!(stderr(&unconfigured).contains("operator plane is not configured"));

    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&fixture.manifest).unwrap()).unwrap();
    manifest["adaptive"] = json!({
        "mode": "feedback_only",
        "feedback": {
            "limits": {
                "max_target_windows": 1,
                "workload": {
                    "max_query_shapes": 1,
                    "max_plan_variants_per_shape": 1
                },
                "max_calibration_epochs": 1,
                "max_calibration_query_shapes": 1,
                "max_calibration_plan_variants_per_shape": 1
            }
        }
    });
    manifest["physical_design"] = json!({
        "evidence_limits": {
            "max_index_candidates": 1,
            "max_columnar_candidates": 1,
            "max_query_shapes_per_candidate": 1,
            "max_columnar_columns_per_candidate": 1
        },
        "advisor_policy": {
            "index": {
                "minimum_reports": 0,
                "minimum_distinct_query_shapes": 0,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 0
            },
            "columnar": {
                "minimum_reports": 0,
                "minimum_distinct_query_shapes": 0,
                "minimum_actual_scan_work_units": 0,
                "max_recommendations": 0
            }
        }
    });
    manifest["operator"] = json!({
        "unix_socket": "offline.sock",
        "io_timeout_ms": 50,
        "allow_physical_index_apply": false
    });
    std::fs::write(&fixture.manifest, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let inspected = catalog(&fixture, "text");
    assert!(inspected.status.success(), "{}", stderr(&inspected));
    assert!(!fixture.directory.join("offline.sock").exists());
    let offline = netbadb()
        .args(["operator", "status", "--manifest"])
        .arg(&fixture.manifest)
        .output()
        .unwrap();
    assert_eq!(offline.status.code(), Some(1));
    assert!(stderr(&offline).contains("failed to connect to operator socket"));
}
