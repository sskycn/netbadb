//! Diagnostics-only synchronous NetbaDB language server.

use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use lsp_server::{Connection, ErrorCode, Message, Notification, Response};
use lsp_types::notification::{
    DidChangeTextDocument, DidCloseTextDocument, DidOpenTextDocument, Exit, LogMessage,
    Notification as _, PublishDiagnostics,
};
use lsp_types::{
    Diagnostic, DiagnosticSeverity, DidChangeTextDocumentParams, DidCloseTextDocumentParams,
    DidOpenTextDocumentParams, GeneralClientCapabilities, InitializeParams, InitializeResult,
    LogMessageParams, MessageType, NumberOrString, Position, PositionEncodingKind,
    PublishDiagnosticsParams, Range, ServerCapabilities, ServerInfo, TextDocumentSyncCapability,
    TextDocumentSyncKind, Uri,
};
use netbadb_schema_spec::{Schema, SchemaSpecError, parse_schema_spec};
use netbadb_tooling::{TextSpan, diagnose_statement};

const HELP: &str = "Usage: netbadb-lsp --schema <schema.json>\n\nDiagnostics-only NetbaDB language server over stdio.\n";

/// Parses CLI input, loads the Schema Spec before protocol startup, and runs
/// the stdio language-server lifecycle. Help and version output are returned to
/// the caller; protocol output is owned exclusively by `lsp-server`.
pub fn run_cli(arguments: impl IntoIterator<Item = OsString>) -> Result<Option<String>, LspError> {
    match parse_args(arguments)? {
        Action::Help => Ok(Some(HELP.to_owned())),
        Action::Version => Ok(Some(format!("netbadb-lsp {}\n", env!("CARGO_PKG_VERSION")))),
        Action::Serve(schema_path) => {
            let source =
                std::fs::read_to_string(&schema_path).map_err(|source| LspError::ReadSchema {
                    path: schema_path,
                    source,
                })?;
            let schema = parse_schema_spec(&source).map_err(LspError::SchemaSpec)?;
            let (connection, io_threads) = Connection::stdio();
            let server_result = run_server(connection, schema);
            let io_result = io_threads.join().map_err(LspError::Io);
            server_result?;
            io_result?;
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Help,
    Version,
    Serve(PathBuf),
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, LspError> {
    let mut arguments = arguments.into_iter();
    let first = arguments
        .next()
        .ok_or_else(|| LspError::Usage("--schema is required".into()))?;
    if first == "--help" || first == "-h" {
        return no_extra(arguments, Action::Help);
    }
    if first == "--version" || first == "-V" {
        return no_extra(arguments, Action::Version);
    }
    if first != "--schema" {
        return Err(LspError::Usage(format!(
            "unknown argument `{}`",
            first.to_string_lossy()
        )));
    }
    let path = arguments
        .next()
        .ok_or_else(|| LspError::Usage("--schema requires a path".into()))?;
    no_extra(arguments, Action::Serve(PathBuf::from(path)))
}

fn no_extra(
    mut arguments: impl Iterator<Item = OsString>,
    action: Action,
) -> Result<Action, LspError> {
    match arguments.next() {
        Some(argument) => Err(LspError::Usage(format!(
            "unexpected additional argument `{}`",
            argument.to_string_lossy()
        ))),
        None => Ok(action),
    }
}

/// Runs one initialized, diagnostics-only LSP connection against a fixed
/// canonical schema. The caller owns the transport construction.
pub fn run_server(connection: Connection, schema: Schema) -> Result<(), LspError> {
    initialize(&connection)?;
    let mut server = LspServer {
        connection,
        schema,
        documents: HashMap::new(),
    };
    server.run()
}

fn initialize(connection: &Connection) -> Result<(), LspError> {
    let (id, params) = connection.initialize_start().map_err(LspError::Protocol)?;
    let params: InitializeParams = match serde_json::from_value(params) {
        Ok(params) => params,
        Err(error) => {
            send_response(
                connection,
                Response::new_err(
                    id,
                    ErrorCode::InvalidParams as i32,
                    format!("invalid initialize parameters: {error}"),
                ),
            )?;
            return Err(LspError::Json(error));
        }
    };
    if explicitly_rejects_utf16(params.capabilities.general.as_ref()) {
        let message = "client does not advertise required UTF-16 position encoding";
        send_response(
            connection,
            Response::new_err(id, ErrorCode::InvalidParams as i32, message.into()),
        )?;
        return Err(LspError::UnsupportedPositionEncoding);
    }
    let result = InitializeResult {
        capabilities: ServerCapabilities {
            position_encoding: Some(PositionEncodingKind::UTF16),
            text_document_sync: Some(TextDocumentSyncCapability::Kind(TextDocumentSyncKind::FULL)),
            ..ServerCapabilities::default()
        },
        server_info: Some(ServerInfo {
            name: "netbadb-lsp".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
        }),
    };
    let result = serde_json::to_value(result).map_err(LspError::Json)?;
    connection
        .initialize_finish(id, result)
        .map_err(LspError::Protocol)
}

fn explicitly_rejects_utf16(general: Option<&GeneralClientCapabilities>) -> bool {
    general
        .and_then(|general| general.position_encodings.as_ref())
        .is_some_and(|encodings| {
            !encodings
                .iter()
                .any(|encoding| encoding == &PositionEncodingKind::UTF16)
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentState {
    version: i32,
    text: String,
}

struct LspServer {
    connection: Connection,
    schema: Schema,
    documents: HashMap<Uri, DocumentState>,
}

impl LspServer {
    fn run(&mut self) -> Result<(), LspError> {
        while let Ok(message) = self.connection.receiver.recv() {
            match message {
                Message::Request(request) => {
                    if self
                        .connection
                        .handle_shutdown(&request)
                        .map_err(LspError::Protocol)?
                    {
                        return Ok(());
                    }
                    self.method_not_found(request)?;
                }
                Message::Notification(notification) => {
                    if notification.method == Exit::METHOD {
                        return Err(LspError::ExitWithoutShutdown);
                    }
                    self.handle_notification(notification)?;
                }
                Message::Response(_) => {}
            }
        }
        Err(LspError::TransportClosed)
    }

    fn method_not_found(&self, request: lsp_server::Request) -> Result<(), LspError> {
        let message = format!("unsupported request `{}`", request.method);
        send_response(
            &self.connection,
            Response::new_err(request.id, ErrorCode::MethodNotFound as i32, message),
        )
    }

    fn handle_notification(&mut self, notification: Notification) -> Result<(), LspError> {
        match notification.method.as_str() {
            DidOpenTextDocument::METHOD => {
                let Some(params) = self.decode_notification::<DidOpenTextDocumentParams>(
                    notification.params,
                    DidOpenTextDocument::METHOD,
                )?
                else {
                    return Ok(());
                };
                let document = params.text_document;
                self.documents.insert(
                    document.uri.clone(),
                    DocumentState {
                        version: document.version,
                        text: document.text,
                    },
                );
                self.publish_document(&document.uri)
            }
            DidChangeTextDocument::METHOD => {
                let Some(params) = self.decode_notification::<DidChangeTextDocumentParams>(
                    notification.params,
                    DidChangeTextDocument::METHOD,
                )?
                else {
                    return Ok(());
                };
                self.change_document(params)
            }
            DidCloseTextDocument::METHOD => {
                let Some(params) = self.decode_notification::<DidCloseTextDocumentParams>(
                    notification.params,
                    DidCloseTextDocument::METHOD,
                )?
                else {
                    return Ok(());
                };
                let uri = params.text_document.uri;
                self.documents.remove(&uri);
                self.publish(uri, Vec::new(), None)
            }
            _ => Ok(()),
        }
    }

    fn decode_notification<T>(
        &self,
        params: serde_json::Value,
        method: &'static str,
    ) -> Result<Option<T>, LspError>
    where
        T: serde::de::DeserializeOwned,
    {
        match serde_json::from_value(params) {
            Ok(params) => Ok(Some(params)),
            Err(error) => {
                self.log_error(format!("ignored invalid {method} notification: {error}"))?;
                Ok(None)
            }
        }
    }

    fn change_document(&mut self, params: DidChangeTextDocumentParams) -> Result<(), LspError> {
        let uri = params.text_document.uri;
        let version = params.text_document.version;
        let Some(current) = self.documents.get(&uri) else {
            return self.log_error("ignored change for unopened document".into());
        };
        if version <= current.version {
            return Ok(());
        }
        let mut changes = params.content_changes.into_iter();
        let Some(change) = changes.next() else {
            return self.log_error("ignored full-sync change without content".into());
        };
        if changes.next().is_some() || change.range.is_some() || change.range_length.is_some() {
            return self.log_error("ignored incremental edit; server requires full sync".into());
        }
        self.documents.insert(
            uri.clone(),
            DocumentState {
                version,
                text: change.text,
            },
        );
        self.publish_document(&uri)
    }

    fn publish_document(&self, uri: &Uri) -> Result<(), LspError> {
        let document = self
            .documents
            .get(uri)
            .ok_or(LspError::DocumentStateMissing)?;
        let diagnostics =
            diagnostics_for_document(&self.schema, &document.text).map_err(LspError::Position)?;
        self.publish(uri.clone(), diagnostics, Some(document.version))
    }

    fn publish(
        &self,
        uri: Uri,
        diagnostics: Vec<Diagnostic>,
        version: Option<i32>,
    ) -> Result<(), LspError> {
        send_notification(
            &self.connection,
            PublishDiagnostics::METHOD,
            PublishDiagnosticsParams::new(uri, diagnostics, version),
        )
    }

    fn log_error(&self, message: String) -> Result<(), LspError> {
        send_notification(
            &self.connection,
            LogMessage::METHOD,
            LogMessageParams {
                typ: MessageType::ERROR,
                message,
            },
        )
    }
}

fn send_response(connection: &Connection, response: Response) -> Result<(), LspError> {
    connection
        .sender
        .send(Message::Response(response))
        .map_err(|_| LspError::TransportClosed)
}

fn send_notification(
    connection: &Connection,
    method: &'static str,
    params: impl serde::Serialize,
) -> Result<(), LspError> {
    let params = serde_json::to_value(params).map_err(LspError::Json)?;
    connection
        .sender
        .send(Message::Notification(Notification {
            method: method.into(),
            params,
        }))
        .map_err(|_| LspError::TransportClosed)
}

/// Maps schema-driven diagnostics to LSP diagnostics for one editor buffer.
pub fn diagnostics_for_document(
    schema: &Schema,
    source: &str,
) -> Result<Vec<Diagnostic>, PositionError> {
    diagnose_statement(schema, source)
        .into_iter()
        .map(|diagnostic| {
            Ok(Diagnostic {
                range: span_to_range(source, diagnostic.span)?,
                severity: Some(DiagnosticSeverity::ERROR),
                code: Some(NumberOrString::String(diagnostic.code.as_str().into())),
                code_description: None,
                source: Some("netbadb".into()),
                message: diagnostic.message,
                related_information: None,
                tags: None,
                data: None,
            })
        })
        .collect()
}

/// Converts one checked UTF-8 byte offset to a zero-based UTF-16 LSP position.
pub fn byte_offset_to_position(source: &str, offset: usize) -> Result<Position, PositionError> {
    if offset > source.len() {
        return Err(PositionError::OffsetOutOfBounds {
            offset,
            source_len: source.len(),
        });
    }
    if !source.is_char_boundary(offset) {
        return Err(PositionError::NotCharBoundary { offset });
    }

    let mut line = 0_usize;
    let mut character = 0_usize;
    let mut index = 0_usize;
    while index < offset {
        let remaining = &source[index..];
        if remaining.starts_with("\r\n") {
            if offset == index + 1 {
                return Err(PositionError::BetweenCrLf { offset });
            }
            line = line.checked_add(1).ok_or(PositionError::LineOverflow)?;
            character = 0;
            index += 2;
            continue;
        }
        let character_value = remaining
            .chars()
            .next()
            .ok_or(PositionError::OffsetOutOfBounds {
                offset,
                source_len: source.len(),
            })?;
        index += character_value.len_utf8();
        if character_value == '\n' || character_value == '\r' {
            line = line.checked_add(1).ok_or(PositionError::LineOverflow)?;
            character = 0;
        } else {
            character = character
                .checked_add(character_value.len_utf16())
                .ok_or(PositionError::CharacterOverflow)?;
        }
    }

    Ok(Position {
        line: u32::try_from(line).map_err(|_| PositionError::LineOverflow)?,
        character: u32::try_from(character).map_err(|_| PositionError::CharacterOverflow)?,
    })
}

/// Converts a checked tooling byte span to a UTF-16 LSP range.
pub fn span_to_range(source: &str, span: TextSpan) -> Result<Range, PositionError> {
    if span.start > span.end {
        return Err(PositionError::ReversedSpan {
            start: span.start,
            end: span.end,
        });
    }
    Ok(Range {
        start: byte_offset_to_position(source, span.start)?,
        end: byte_offset_to_position(source, span.end)?,
    })
}

/// A checked byte-to-LSP position conversion failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionError {
    ReversedSpan { start: usize, end: usize },
    OffsetOutOfBounds { offset: usize, source_len: usize },
    NotCharBoundary { offset: usize },
    BetweenCrLf { offset: usize },
    LineOverflow,
    CharacterOverflow,
}

impl fmt::Display for PositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReversedSpan { start, end } => {
                write!(formatter, "text span is reversed: {start}..{end}")
            }
            Self::OffsetOutOfBounds { offset, source_len } => write!(
                formatter,
                "byte offset {offset} exceeds source length {source_len}"
            ),
            Self::NotCharBoundary { offset } => {
                write!(
                    formatter,
                    "byte offset {offset} is not a UTF-8 character boundary"
                )
            }
            Self::BetweenCrLf { offset } => {
                write!(
                    formatter,
                    "byte offset {offset} falls inside a CRLF sequence"
                )
            }
            Self::LineOverflow => formatter.write_str("LSP line number exceeds u32"),
            Self::CharacterOverflow => {
                formatter.write_str("LSP UTF-16 character offset exceeds u32")
            }
        }
    }
}

impl Error for PositionError {}

/// CLI, startup, protocol, or internal adapter failure.
#[derive(Debug)]
pub enum LspError {
    Usage(String),
    ReadSchema {
        path: PathBuf,
        source: std::io::Error,
    },
    SchemaSpec(SchemaSpecError),
    Json(serde_json::Error),
    Protocol(lsp_server::ProtocolError),
    Io(std::io::Error),
    Position(PositionError),
    UnsupportedPositionEncoding,
    DocumentStateMissing,
    ExitWithoutShutdown,
    TransportClosed,
}

impl fmt::Display for LspError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => formatter.write_str(message),
            Self::ReadSchema { path, source } => write!(
                formatter,
                "failed to read SDK Schema Spec `{}`: {source}",
                path.display()
            ),
            Self::SchemaSpec(error) => error.fmt(formatter),
            Self::Json(error) => write!(formatter, "invalid LSP data: {error}"),
            Self::Protocol(error) => write!(formatter, "LSP protocol error: {error}"),
            Self::Io(error) => write!(formatter, "LSP stdio error: {error}"),
            Self::Position(error) => write!(formatter, "invalid compiler span: {error}"),
            Self::UnsupportedPositionEncoding => {
                formatter.write_str("client does not support UTF-16 LSP positions")
            }
            Self::DocumentStateMissing => {
                formatter.write_str("open document state disappeared before diagnostics")
            }
            Self::ExitWithoutShutdown => formatter.write_str("client sent exit before shutdown"),
            Self::TransportClosed => formatter.write_str("LSP transport closed unexpectedly"),
        }
    }
}

impl Error for LspError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadSchema { source, .. } | Self::Io(source) => Some(source),
            Self::SchemaSpec(source) => Some(source),
            Self::Json(source) => Some(source),
            Self::Protocol(source) => Some(source),
            Self::Position(source) => Some(source),
            Self::Usage(_)
            | Self::UnsupportedPositionEncoding
            | Self::DocumentStateMissing
            | Self::ExitWithoutShutdown
            | Self::TransportClosed => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use lsp_server::{Message, Request};
    use lsp_types::notification::{
        DidChangeTextDocument, DidCloseTextDocument, DidOpenTextDocument, Exit, Initialized,
        Notification as _, PublishDiagnostics,
    };
    use lsp_types::request::{Initialize, Request as _, Shutdown};
    use lsp_types::{
        ClientCapabilities, DidChangeTextDocumentParams, DidCloseTextDocumentParams,
        DidOpenTextDocumentParams, GeneralClientCapabilities, InitializeParams, InitializedParams,
        PublishDiagnosticsParams, TextDocumentContentChangeEvent, TextDocumentIdentifier,
        TextDocumentItem, VersionedTextDocumentIdentifier,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![TableDef::new(
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
                ),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(3),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::Int64,
                    },
                ),
            ],
        )])
        .expect("valid schema")
    }

    fn initialize(client: &Connection) -> InitializeResult {
        let params = InitializeParams {
            capabilities: ClientCapabilities {
                general: Some(GeneralClientCapabilities {
                    position_encodings: Some(vec![PositionEncodingKind::UTF16]),
                    ..GeneralClientCapabilities::default()
                }),
                ..ClientCapabilities::default()
            },
            ..InitializeParams::default()
        };
        client
            .sender
            .send(Message::Request(Request::new(
                1.into(),
                Initialize::METHOD.into(),
                params,
            )))
            .unwrap();
        let Message::Response(response) = client.receiver.recv().unwrap() else {
            panic!("expected initialize response");
        };
        let result = serde_json::from_value(response.result.expect("initialize result")).unwrap();
        client
            .sender
            .send(Message::Notification(Notification::new(
                Initialized::METHOD.into(),
                InitializedParams {},
            )))
            .unwrap();
        result
    }

    fn notify(client: &Connection, method: &str, params: impl serde::Serialize) {
        client
            .sender
            .send(Message::Notification(Notification::new(
                method.into(),
                params,
            )))
            .unwrap();
    }

    fn receive_publish(client: &Connection) -> PublishDiagnosticsParams {
        loop {
            let Message::Notification(notification) = client.receiver.recv().unwrap() else {
                continue;
            };
            if notification.method == PublishDiagnostics::METHOD {
                return serde_json::from_value(notification.params).unwrap();
            }
        }
    }

    fn shutdown(client: &Connection, server: thread::JoinHandle<Result<(), LspError>>) {
        client
            .sender
            .send(Message::Request(Request::new(
                99.into(),
                Shutdown::METHOD.into(),
                (),
            )))
            .unwrap();
        let Message::Response(response) = client.receiver.recv().unwrap() else {
            panic!("expected shutdown response");
        };
        assert!(response.error.is_none());
        notify(client, Exit::METHOD, ());
        server.join().unwrap().unwrap();
    }

    #[test]
    fn converts_utf8_bytes_to_utf16_lines_and_checks_invalid_offsets() {
        let source = "a😀b\r\n用户\nend";
        assert_eq!(
            byte_offset_to_position(source, 0).unwrap(),
            Position::new(0, 0)
        );
        assert_eq!(
            byte_offset_to_position(source, 1).unwrap(),
            Position::new(0, 1)
        );
        assert_eq!(
            byte_offset_to_position(source, 5).unwrap(),
            Position::new(0, 3)
        );
        assert_eq!(
            byte_offset_to_position(source, 8).unwrap(),
            Position::new(1, 0)
        );
        let end_of_users = source.find('\n').unwrap() + 1 + "用户".len();
        assert_eq!(
            byte_offset_to_position(source, end_of_users).unwrap(),
            Position::new(1, 2)
        );
        assert!(matches!(
            byte_offset_to_position(source, 2),
            Err(PositionError::NotCharBoundary { .. })
        ));
        assert!(matches!(
            byte_offset_to_position(source, 7),
            Err(PositionError::BetweenCrLf { .. })
        ));
        assert!(matches!(
            byte_offset_to_position(source, source.len() + 1),
            Err(PositionError::OffsetOutOfBounds { .. })
        ));
        assert!(matches!(
            span_to_range(source, TextSpan { start: 5, end: 1 }),
            Err(PositionError::ReversedSpan { .. })
        ));
    }

    #[test]
    fn supports_zero_width_eof_multiline_and_emoji_ranges() {
        let parse = diagnostics_for_document(&schema(), "SELECT FROM users").unwrap();
        assert_eq!(parse.len(), 1);
        assert_eq!(parse[0].code, Some(NumberOrString::String("parse".into())));
        assert_eq!(parse[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(parse[0].source.as_deref(), Some("netbadb"));
        assert!(!parse[0].message.contains(" at "));

        let eof = "SELECT id FROM";
        let range = span_to_range(
            eof,
            TextSpan {
                start: eof.len(),
                end: eof.len(),
            },
        )
        .unwrap();
        assert_eq!(range.start, range.end);

        let source = "SELECT id\r\nFROM users\r\nWHERE name = '😀' AND missing = 1";
        let diagnostics = diagnostics_for_document(&schema(), source).unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].code,
            Some(NumberOrString::String("unknown_column".into()))
        );
        assert_eq!(diagnostics[0].range.start.line, 2);
        let byte = source.find("missing").unwrap();
        assert_eq!(
            diagnostics[0].range.start,
            byte_offset_to_position(source, byte).unwrap()
        );
        assert!(diagnostics[0].range.start.character < u32::try_from(byte).unwrap());
    }

    #[test]
    fn server_advertises_only_full_sync_and_manages_document_diagnostics() {
        let (server_connection, client) = Connection::memory();
        let server = thread::spawn(move || run_server(server_connection, schema()));
        let result = initialize(&client);
        assert_eq!(
            result.capabilities.position_encoding,
            Some(PositionEncodingKind::UTF16)
        );
        assert_eq!(
            result.capabilities.text_document_sync,
            Some(TextDocumentSyncCapability::Kind(TextDocumentSyncKind::FULL))
        );
        assert!(result.capabilities.completion_provider.is_none());
        assert!(result.capabilities.hover_provider.is_none());
        assert!(result.capabilities.definition_provider.is_none());

        let uri: Uri = "file:///tmp/query.sql".parse().unwrap();
        notify(
            &client,
            DidOpenTextDocument::METHOD,
            DidOpenTextDocumentParams {
                text_document: TextDocumentItem::new(
                    uri.clone(),
                    "sql".into(),
                    5,
                    "SELECT id FROM missing".into(),
                ),
            },
        );
        let opened = receive_publish(&client);
        assert_eq!(opened.version, Some(5));
        assert_eq!(opened.diagnostics.len(), 1);
        assert_eq!(
            opened.diagnostics[0].code,
            Some(NumberOrString::String("unknown_table".into()))
        );

        notify(
            &client,
            DidChangeTextDocument::METHOD,
            DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier::new(uri.clone(), 4),
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "SELECT id FROM users".into(),
                }],
            },
        );
        notify(
            &client,
            DidChangeTextDocument::METHOD,
            DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier::new(uri.clone(), 6),
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "SELECT id FROM users".into(),
                }],
            },
        );
        let changed = receive_publish(&client);
        assert_eq!(changed.version, Some(6));
        assert!(changed.diagnostics.is_empty());

        notify(
            &client,
            DidChangeTextDocument::METHOD,
            DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier::new(uri.clone(), 7),
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: Some(Range::new(Position::new(0, 0), Position::new(0, 1))),
                    range_length: None,
                    text: "X".into(),
                }],
            },
        );
        let Message::Notification(log) = client.receiver.recv().unwrap() else {
            panic!("expected log notification");
        };
        assert_eq!(log.method, LogMessage::METHOD);

        notify(
            &client,
            DidChangeTextDocument::METHOD,
            DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier::new(uri.clone(), 7),
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "SELECT missing FROM users".into(),
                }],
            },
        );
        let replaced = receive_publish(&client);
        assert_eq!(replaced.version, Some(7));
        assert_eq!(
            replaced.diagnostics[0].code,
            Some(NumberOrString::String("unknown_column".into()))
        );

        notify(
            &client,
            DidCloseTextDocument::METHOD,
            DidCloseTextDocumentParams {
                text_document: TextDocumentIdentifier::new(uri),
            },
        );
        let closed = receive_publish(&client);
        assert_eq!(closed.version, None);
        assert!(closed.diagnostics.is_empty());
        shutdown(&client, server);
    }

    #[test]
    fn multiple_documents_are_isolated_and_unknown_requests_are_rejected() {
        let (server_connection, client) = Connection::memory();
        let server = thread::spawn(move || run_server(server_connection, schema()));
        initialize(&client);
        let a: Uri = "file:///tmp/a.sql".parse().unwrap();
        let b: Uri = "file:///tmp/b.sql".parse().unwrap();
        for (uri, text) in [
            (a.clone(), "SELECT id FROM missing"),
            (b.clone(), "SELECT id FROM users"),
        ] {
            notify(
                &client,
                DidOpenTextDocument::METHOD,
                DidOpenTextDocumentParams {
                    text_document: TextDocumentItem::new(uri, "anything".into(), 1, text.into()),
                },
            );
        }
        let first = receive_publish(&client);
        let second = receive_publish(&client);
        assert_eq!(first.uri, a);
        assert_eq!(first.diagnostics.len(), 1);
        assert_eq!(second.uri, b);
        assert!(second.diagnostics.is_empty());

        client
            .sender
            .send(Message::Request(Request::new(
                8.into(),
                "netbadb/physicalPlan".into(),
                (),
            )))
            .unwrap();
        let Message::Response(response) = client.receiver.recv().unwrap() else {
            panic!("expected method-not-found response");
        };
        assert_eq!(
            response.error.expect("request error").code,
            ErrorCode::MethodNotFound as i32
        );
        shutdown(&client, server);
    }

    #[test]
    fn initialize_rejects_an_explicit_non_utf16_client() {
        let (server_connection, client) = Connection::memory();
        let server = thread::spawn(move || run_server(server_connection, schema()));
        let params = InitializeParams {
            capabilities: ClientCapabilities {
                general: Some(GeneralClientCapabilities {
                    position_encodings: Some(vec![PositionEncodingKind::UTF8]),
                    ..GeneralClientCapabilities::default()
                }),
                ..ClientCapabilities::default()
            },
            ..InitializeParams::default()
        };
        client
            .sender
            .send(Message::Request(Request::new(
                1.into(),
                Initialize::METHOD.into(),
                params,
            )))
            .unwrap();
        let Message::Response(response) = client.receiver.recv().unwrap() else {
            panic!("expected initialize rejection");
        };
        assert_eq!(
            response.error.expect("initialize error").code,
            ErrorCode::InvalidParams as i32
        );
        assert!(matches!(
            server.join().unwrap(),
            Err(LspError::UnsupportedPositionEncoding)
        ));
    }

    #[test]
    fn parses_small_command_surface() {
        assert_eq!(parse_args(["--help".into()]).unwrap(), Action::Help);
        assert_eq!(parse_args(["--version".into()]).unwrap(), Action::Version);
        assert_eq!(
            parse_args(["--schema".into(), "schema.json".into()]).unwrap(),
            Action::Serve(PathBuf::from("schema.json"))
        );
        assert!(parse_args(Vec::<OsString>::new()).is_err());
        assert!(parse_args(["--manifest".into(), "server.json".into()]).is_err());
    }
}
