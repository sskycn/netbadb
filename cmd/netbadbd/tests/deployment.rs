use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_core::Database;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{ServerConfig, ServerOperatorClient};
use netbadb_types::{ColumnId, PhysicalType, TableId};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

fn test_directory(transport: &str) -> PathBuf {
    let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "netbadbd-v6-{transport}-{}-{sequence}",
        std::process::id()
    ))
}

fn manifest_fixture(transport: &str) -> (PathBuf, PathBuf, PathBuf) {
    let directory = test_directory(transport);
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(directory.join("data")).unwrap();
    Database::create(
        directory.join("data/users.ndb"),
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::UInt64,
                    },
                )
                .primary_key(true),
            ],
        ),
    )
    .unwrap()
    .close()
    .unwrap();

    let socket = PathBuf::from(format!(
        "/tmp/netbadbd-op-{transport}-{}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket);
    let document = include_str!("../../../docs/server-manifest-v6.md");
    let manifest_source = document
        .split_once("```json\n")
        .and_then(|(_, remainder)| remainder.split_once("\n```"))
        .map(|(example, _)| example)
        .expect("v6 documentation contains a JSON example")
        .replace("127.0.0.1:7878", "127.0.0.1:0")
        .replace(
            "run/netbadb-operator.sock",
            socket
                .to_str()
                .expect("ASCII temporary operator socket path"),
        );
    let manifest = directory.join("server.json");
    std::fs::write(&manifest, manifest_source).unwrap();
    (directory, manifest, socket)
}

fn assert_daemon_uses_driven_manifest(postgres: bool) {
    let transport = if postgres { "postgres" } else { "native" };
    let (directory, manifest, socket) = manifest_fixture(transport);
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    let operator = config.operator_config().unwrap().clone();
    let mut command = Command::new(env!("CARGO_BIN_EXE_netbadbd"));
    command
        .arg("--manifest")
        .arg(&manifest)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if postgres {
        command.arg("--postgres");
    }
    let mut child = command.spawn().unwrap();
    let stderr = child.stderr.take().expect("daemon stderr pipe");
    let mut line = String::new();
    BufReader::new(stderr).read_line(&mut line).unwrap();
    assert!(line.contains("adaptive driven"), "startup line: {line}");
    let status = ServerOperatorClient::new(&operator).status().unwrap();
    assert!(status.driver.is_some());
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(socket.exists(), "an abrupt kill leaves a stale socket");
    std::fs::remove_file(socket).unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn native_daemon_uses_v6_driven_and_nbop_without_an_adaptive_flag() {
    assert_daemon_uses_driven_manifest(false);
}

#[test]
fn postgres_daemon_uses_v6_driven_and_nbop_without_an_adaptive_flag() {
    assert_daemon_uses_driven_manifest(true);
}
