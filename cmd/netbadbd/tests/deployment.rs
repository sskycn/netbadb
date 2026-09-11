#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use netbadb_core::Database;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{ServerConfig, ServerOperatorClient};
use netbadb_types::{ColumnId, PhysicalType, TableId};

const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    directory: PathBuf,
    manifest: PathBuf,
    heap: PathBuf,
    table: TableDef,
    socket: Option<PathBuf>,
}

impl Fixture {
    fn cleanup(self) {
        if let Some(socket) = self.socket {
            let _ = std::fs::remove_file(socket);
        }
        std::fs::remove_dir_all(self.directory).unwrap();
    }
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
                    physical: PhysicalType::UInt64,
                },
            )
            .primary_key(true),
        ],
    )
}

fn manifest_fixture(
    transport: &str,
    driven: bool,
    with_physical_design: bool,
    with_operator: bool,
) -> Fixture {
    let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "netbadbd-v7-{transport}-{}-{sequence}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(directory.join("data")).unwrap();
    let heap = directory.join("data/users.ndb");
    let table = users_table();
    Database::create(&heap, table.clone())
        .unwrap()
        .close()
        .unwrap();

    let socket = PathBuf::from(format!(
        "/tmp/netbadbd-op-{transport}-{}-{sequence}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&socket);
    let document = include_str!("../../../docs/server-manifest-v7.md");
    let example = document
        .split_once("```json\n")
        .and_then(|(_, remainder)| remainder.split_once("\n```"))
        .map(|(example, _)| example)
        .expect("v7 documentation contains a JSON example");
    let mut source: serde_json::Value = serde_json::from_str(example).unwrap();
    source["listen"] = "127.0.0.1:0".into();
    source["operator"]["unix_socket"] = socket.to_string_lossy().into_owned().into();
    let object = source.as_object_mut().unwrap();
    if !driven {
        object.remove("adaptive");
    }
    if !with_physical_design {
        object.remove("physical_design");
    }
    if !with_operator {
        object.remove("operator");
    }
    let manifest = directory.join("server.json");
    std::fs::write(&manifest, serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    Fixture {
        directory,
        manifest,
        heap,
        table,
        socket: with_operator.then_some(socket),
    }
}

struct DaemonProcess {
    child: Child,
    lines: Receiver<String>,
    reader: JoinHandle<Vec<String>>,
}

impl DaemonProcess {
    fn spawn(manifest: &Path, postgres: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_netbadbd"));
        command
            .arg("--manifest")
            .arg(manifest)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if postgres {
            command.arg("--postgres");
        }
        let mut child = command.spawn().unwrap();
        let stderr = child.stderr.take().expect("daemon stderr pipe");
        let (sender, lines) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut captured = Vec::new();
            for line in BufReader::new(stderr).lines() {
                let line = line.expect("read daemon stderr");
                let _ = sender.send(line.clone());
                captured.push(line);
            }
            captured
        });
        Self {
            child,
            lines,
            reader,
        }
    }

    fn wait_for_readiness(&mut self) -> String {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let mut observed = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(line) if line.starts_with("netbadbd ready:") => return line,
                Ok(line) => observed.push(line),
                Err(RecvTimeoutError::Disconnected) => {
                    let status = self.child.wait().unwrap();
                    panic!("daemon exited before readiness with {status}: {observed:?}");
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.child.kill().unwrap();
                    let _ = self.child.wait();
                    panic!("daemon did not publish readiness within {PROCESS_TIMEOUT:?}");
                }
            }
        }
    }

    fn send_signal(&self, signal: &str) {
        let status = Command::new("python3")
            .arg("-c")
            .arg("import os, signal, sys; os.kill(int(sys.argv[1]), getattr(signal, sys.argv[2]))")
            .arg(self.child.id().to_string())
            .arg(signal)
            .status()
            .unwrap();
        assert!(status.success(), "failed to send {signal} to daemon");
    }

    fn send_signal_twice(&self, signal: &str) {
        let status = Command::new("python3")
            .arg("-c")
            .arg(
                "import os, signal, sys; value = getattr(signal, sys.argv[2]); os.kill(int(sys.argv[1]), value); os.kill(int(sys.argv[1]), value)",
            )
            .arg(self.child.id().to_string())
            .arg(signal)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "failed to send repeated {signal} to daemon"
        );
    }

    fn wait(mut self) -> (ExitStatus, Vec<String>) {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                self.child.kill().unwrap();
                let _ = self.child.wait();
                panic!("daemon did not exit within {PROCESS_TIMEOUT:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        (status, self.reader.join().unwrap())
    }
}

fn assert_driven_operator(fixture: &Fixture) {
    let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
    let operator = config.operator_config().unwrap();
    let status = ServerOperatorClient::new(operator).status().unwrap();
    assert!(status.adaptive.unwrap().driver.is_some());
    assert!(status.physical_design.is_some());
}

#[test]
fn native_sigterm_gracefully_closes_driven_daemon_and_removes_operator_socket() {
    let fixture = manifest_fixture("native-sigterm", true, true, true);
    let mut daemon = DaemonProcess::spawn(&fixture.manifest, false);
    let ready = daemon.wait_for_readiness();
    assert!(ready.contains("native listener"), "readiness line: {ready}");
    assert!(ready.contains("adaptive driven"), "readiness line: {ready}");
    assert!(
        ready.contains("physical-design enabled"),
        "readiness line: {ready}"
    );
    assert!(!ready.contains(fixture.socket.as_ref().unwrap().to_string_lossy().as_ref()));
    assert_driven_operator(&fixture);

    daemon.send_signal_twice("SIGTERM");
    let (status, _) = daemon.wait();
    assert!(status.success(), "graceful SIGTERM status: {status}");
    assert!(
        !fixture.socket.as_ref().unwrap().exists(),
        "graceful shutdown removes the owned operator socket"
    );
    let reopened = Database::open(&fixture.heap, fixture.table.clone()).unwrap();
    reopened.close().unwrap();
    fixture.cleanup();
}

#[test]
fn native_sigint_gracefully_closes_disabled_daemon_without_operator() {
    let fixture = manifest_fixture("native-sigint", false, false, false);
    let mut daemon = DaemonProcess::spawn(&fixture.manifest, false);
    let ready = daemon.wait_for_readiness();
    assert!(
        ready.contains("adaptive disabled"),
        "readiness line: {ready}"
    );
    assert!(
        ready.contains("physical-design disabled"),
        "readiness line: {ready}"
    );

    daemon.send_signal("SIGINT");
    let (status, _) = daemon.wait();
    assert!(status.success(), "graceful SIGINT status: {status}");
    fixture.cleanup();
}

#[test]
fn postgres_sigterm_gracefully_closes_driven_daemon() {
    let fixture = manifest_fixture("postgres-sigterm", true, true, true);
    let mut daemon = DaemonProcess::spawn(&fixture.manifest, true);
    let ready = daemon.wait_for_readiness();
    assert!(
        ready.contains("PostgreSQL listener"),
        "readiness line: {ready}"
    );
    assert!(ready.contains("adaptive driven"), "readiness line: {ready}");
    assert!(
        ready.contains("physical-design enabled"),
        "readiness line: {ready}"
    );
    assert_driven_operator(&fixture);

    daemon.send_signal("SIGTERM");
    let (status, _) = daemon.wait();
    assert!(status.success(), "graceful SIGTERM status: {status}");
    assert!(!fixture.socket.as_ref().unwrap().exists());
    fixture.cleanup();
}

#[test]
fn operator_startup_failure_never_publishes_readiness() {
    let fixture = manifest_fixture("operator-conflict", true, true, true);
    let socket = fixture.socket.as_ref().unwrap();
    std::fs::write(socket, b"owned elsewhere").unwrap();

    let daemon = DaemonProcess::spawn(&fixture.manifest, false);
    let (status, lines) = daemon.wait();
    assert!(!status.success());
    assert!(
        lines
            .iter()
            .all(|line| !line.starts_with("netbadbd ready:")),
        "startup failure output: {lines:?}"
    );
    assert_eq!(std::fs::read(socket).unwrap(), b"owned elsewhere");
    fixture.cleanup();
}

#[test]
fn native_design_only_daemon_exposes_status_and_conditional_rotation() {
    let fixture = manifest_fixture("native-design-only", false, true, true);
    let mut daemon = DaemonProcess::spawn(&fixture.manifest, false);
    let ready = daemon.wait_for_readiness();
    assert!(
        ready.contains("adaptive disabled"),
        "readiness line: {ready}"
    );
    assert!(
        ready.contains("physical-design enabled"),
        "readiness line: {ready}"
    );

    let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
    let operator = ServerOperatorClient::new(config.operator_config().unwrap());
    let status = operator.status().unwrap();
    assert!(status.adaptive.is_none());
    assert_eq!(status.physical_design.unwrap().evidence.epoch, 0);
    let rotation = operator.rotate_physical_design_evidence(0).unwrap();
    assert_eq!(rotation.previous_epoch, 0);
    assert_eq!(rotation.new_epoch, 1);

    daemon.send_signal("SIGTERM");
    let (status, _) = daemon.wait();
    assert!(status.success());
    assert!(!fixture.socket.as_ref().unwrap().exists());
    fixture.cleanup();
}

#[test]
fn postgres_design_only_daemon_exposes_nbop_v2_status() {
    let fixture = manifest_fixture("postgres-design-only", false, true, true);
    let mut daemon = DaemonProcess::spawn(&fixture.manifest, true);
    let ready = daemon.wait_for_readiness();
    assert!(
        ready.contains("adaptive disabled"),
        "readiness line: {ready}"
    );
    assert!(
        ready.contains("physical-design enabled"),
        "readiness line: {ready}"
    );
    let config = ServerConfig::from_manifest_path(&fixture.manifest).unwrap();
    let status = ServerOperatorClient::new(config.operator_config().unwrap())
        .status()
        .unwrap();
    assert!(status.adaptive.is_none());
    assert!(status.physical_design.is_some());

    daemon.send_signal("SIGTERM");
    let (status, _) = daemon.wait();
    assert!(status.success());
    fixture.cleanup();
}
