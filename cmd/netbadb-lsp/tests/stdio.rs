use std::io::{BufReader, Cursor, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use lsp_server::{ErrorCode, Message, Notification, Request};
use lsp_types::notification::{
    DidChangeTextDocument, DidOpenTextDocument, Exit, Initialized, Notification as _,
    PublishDiagnostics,
};
use lsp_types::request::{Initialize, Request as _, Shutdown};
use lsp_types::{
    ClientCapabilities, DidChangeTextDocumentParams, DidOpenTextDocumentParams,
    GeneralClientCapabilities, InitializeParams, InitializeResult, InitializedParams,
    NumberOrString, PositionEncodingKind, PublishDiagnosticsParams, TextDocumentContentChangeEvent,
    TextDocumentItem, Uri, VersionedTextDocumentIdentifier,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

const SCHEMA_SPEC: &str = r#"{
  "version": 1,
  "tables": [{
    "id": 1,
    "name": "users",
    "columns": [
      {
        "id": 1,
        "name": "id",
        "physical_type": "int64",
        "semantic_type": "UserId",
        "nullable": false,
        "primary_key": true
      },
      {
        "id": 2,
        "name": "name",
        "physical_type": "text",
        "semantic_type": null,
        "nullable": false,
        "primary_key": false
      }
    ]
  }]
}"#;

struct Fixture {
    directory: PathBuf,
    schema: PathBuf,
    query: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "netbadb-lsp-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let schema = directory.join("schema.json");
        let query = directory.join("query.sql");
        std::fs::write(&schema, SCHEMA_SPEC).unwrap();
        std::fs::write(&query, "SELECT broken FROM disk_only").unwrap();
        Self {
            directory,
            schema,
            query,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn netbadb_lsp() -> Command {
    Command::new(env!("CARGO_BIN_EXE_netbadb-lsp"))
}

fn encode(messages: &[Message]) -> Vec<u8> {
    let mut output = Vec::new();
    for message in messages {
        message.write(&mut output).unwrap();
    }
    output
}

fn decode(bytes: &[u8]) -> Vec<Message> {
    let mut reader = BufReader::new(Cursor::new(bytes));
    let mut messages = Vec::new();
    while let Some(message) = Message::read(&mut reader).unwrap() {
        messages.push(message);
    }
    messages
}

#[test]
fn binary_stdio_uses_editor_text_and_emits_only_lsp_frames() {
    let fixture = Fixture::new("stdio");
    let uri: Uri = format!("file://{}", fixture.query.display())
        .parse()
        .unwrap();
    let messages = vec![
        Message::Request(Request::new(
            1.into(),
            Initialize::METHOD.into(),
            InitializeParams::default(),
        )),
        Message::Notification(Notification::new(
            Initialized::METHOD.into(),
            InitializedParams {},
        )),
        Message::Notification(Notification::new(
            DidOpenTextDocument::METHOD.into(),
            DidOpenTextDocumentParams {
                text_document: TextDocumentItem::new(
                    uri.clone(),
                    "custom-language-id".into(),
                    1,
                    "SELECT id FROM users".into(),
                ),
            },
        )),
        Message::Notification(Notification::new(
            DidChangeTextDocument::METHOD.into(),
            DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier::new(uri.clone(), 2),
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "SELECT id FROM missing".into(),
                }],
            },
        )),
        Message::Request(Request::new(2.into(), Shutdown::METHOD.into(), ())),
        Message::Notification(Notification::new(Exit::METHOD.into(), ())),
    ];

    let mut child = netbadb_lsp()
        .arg("--schema")
        .arg(&fixture.schema)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&encode(&messages))
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let messages = decode(&output.stdout);
    assert_eq!(messages.len(), 4);
    let Message::Response(initialize) = &messages[0] else {
        panic!("expected initialize response");
    };
    let result: InitializeResult =
        serde_json::from_value(initialize.result.clone().unwrap()).unwrap();
    assert_eq!(result.server_info.unwrap().name, "netbadb-lsp");
    assert!(result.capabilities.completion_provider.is_none());

    let Message::Notification(opened) = &messages[1] else {
        panic!("expected open diagnostics");
    };
    assert_eq!(opened.method, PublishDiagnostics::METHOD);
    let opened: PublishDiagnosticsParams = serde_json::from_value(opened.params.clone()).unwrap();
    assert_eq!(opened.uri, uri);
    assert_eq!(opened.version, Some(1));
    assert!(opened.diagnostics.is_empty());

    let Message::Notification(changed) = &messages[2] else {
        panic!("expected change diagnostics");
    };
    let changed: PublishDiagnosticsParams = serde_json::from_value(changed.params.clone()).unwrap();
    assert_eq!(changed.version, Some(2));
    assert_eq!(changed.diagnostics.len(), 1);
    assert_eq!(
        changed.diagnostics[0].code,
        Some(NumberOrString::String("unknown_table".into()))
    );
    assert_eq!(changed.diagnostics[0].source.as_deref(), Some("netbadb"));

    let Message::Response(shutdown) = &messages[3] else {
        panic!("expected shutdown response");
    };
    assert!(shutdown.error.is_none());
}

#[test]
fn invalid_schema_fails_before_protocol_startup() {
    let fixture = Fixture::new("invalid-schema");
    for (source, expected) in [
        (
            r#"{"version":1,"unknown":true,"tables":[]}"#,
            "invalid SDK Schema Spec JSON",
        ),
        (
            r#"{"version":2,"tables":[]}"#,
            "unsupported SDK Schema Spec version 2",
        ),
        (
            r#"{"version":1,"tables":[{"id":1,"name":"a","columns":[]},{"id":1,"name":"b","columns":[]}]}"#,
            "invalid canonical schema",
        ),
        (
            r#"{"version":1,"tables":[{"id":1,"name":"a","columns":[{"id":1,"name":"id","physical_type":"i64","semantic_type":null,"nullable":false,"primary_key":true}]}]}"#,
            "invalid SDK Schema Spec JSON",
        ),
    ] {
        std::fs::write(&fixture.schema, source).unwrap();
        let output = netbadb_lsp()
            .arg("--schema")
            .arg(&fixture.schema)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr).unwrap().contains(expected));
    }
}

#[test]
fn rejected_position_encoding_flushes_a_complete_error_response() {
    let fixture = Fixture::new("unsupported-position-encoding");
    let initialize = Message::Request(Request::new(
        1.into(),
        Initialize::METHOD.into(),
        InitializeParams {
            capabilities: ClientCapabilities {
                general: Some(GeneralClientCapabilities {
                    position_encodings: Some(vec![PositionEncodingKind::UTF8]),
                    ..GeneralClientCapabilities::default()
                }),
                ..ClientCapabilities::default()
            },
            ..InitializeParams::default()
        },
    ));
    let mut child = netbadb_lsp()
        .arg("--schema")
        .arg(&fixture.schema)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&encode(&[initialize]))
        .unwrap();

    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("client does not support UTF-16 LSP positions")
    );
    let messages = decode(&output.stdout);
    assert_eq!(messages.len(), 1);
    let Message::Response(response) = &messages[0] else {
        panic!("expected initialize rejection response");
    };
    assert_eq!(
        response.error.as_ref().expect("initialize error").code,
        ErrorCode::InvalidParams as i32
    );
}

#[test]
fn help_and_version_do_not_enter_stdio_protocol() {
    for argument in ["--help", "--version"] {
        let output = netbadb_lsp().arg(argument).output().unwrap();
        assert!(output.status.success());
        assert!(!output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
}
