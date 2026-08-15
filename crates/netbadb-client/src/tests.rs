use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use netbadb_protocol::{
    CAPABILITY_STREAMED_QUERY_RESULTS, ClientMessage, Frame, MAX_FRAME_PAYLOAD,
    SERVER_CAPABILITIES, ServerMessage, TableSchemaIdentity, WireResultColumn, read_client_frame,
    write_server_frame,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};

use super::*;

fn users_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Semantic {
                name: "UserId".into(),
                physical: PhysicalType::Int64,
            },
        )],
    )
}

fn scripted_server(script: impl FnOnce(TcpStream) + Send + 'static) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let join = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        script(stream);
    });
    (address, join)
}

fn read_request(stream: &mut TcpStream) -> Frame<ClientMessage> {
    read_client_frame(stream).unwrap().expect("client request")
}

fn write_response(stream: &mut TcpStream, request_id: u64, message: ServerMessage) {
    write_server_frame(
        stream,
        &Frame {
            request_id,
            message,
        },
    )
    .unwrap();
}

fn hello(stream: &mut TcpStream, capabilities: u64, tables: Vec<TableSchemaIdentity>) {
    let request = read_request(stream);
    assert_eq!(request.request_id, 1);
    assert_eq!(request.message, ClientMessage::Hello);
    write_response(
        stream,
        request.request_id,
        ServerMessage::HelloAck {
            protocol_version: PROTOCOL_VERSION,
            max_frame_payload: MAX_FRAME_PAYLOAD,
            capabilities,
            tables,
        },
    );
}

fn connect_scripted(script: impl FnOnce(TcpStream) + Send + 'static) -> (Client, JoinHandle<()>) {
    let (address, join) = scripted_server(script);
    (Client::connect(Config::new(address)).unwrap(), join)
}

fn query_columns(nullable: bool) -> Vec<WireResultColumn> {
    vec![WireResultColumn {
        name: "id".into(),
        data_type: SemanticType::named("UserId", PhysicalType::Int64),
        nullable,
    }]
}

#[test]
fn table_identity_uses_canonical_schema_fingerprint() {
    let table = users_table();
    assert_eq!(
        TableIdentity::from_table(&table).unwrap(),
        TableIdentity {
            table_id: table.id,
            fingerprint: table.fingerprint().unwrap(),
        }
    );
}

#[test]
fn plaintext_validation_accepts_only_resolved_loopback_ips() {
    assert_eq!(
        validate_plaintext_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        Ok(())
    );
    assert_eq!(
        validate_plaintext_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        Ok(())
    );
    assert_eq!(
        validate_plaintext_ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))),
        Err(())
    );
}

#[test]
fn handshake_enforces_capability_and_minimum_schema_gates() {
    let required = TableIdentity::from_table(&users_table()).unwrap();
    let extra = TableSchemaIdentity {
        table_id: TableId(2),
        fingerprint: [2; 32],
    };
    let expected = *required.fingerprint.as_bytes();
    let (address, join) = scripted_server(move |mut stream| {
        hello(
            &mut stream,
            SERVER_CAPABILITIES,
            vec![
                TableSchemaIdentity {
                    table_id: TableId(1),
                    fingerprint: expected,
                },
                extra,
            ],
        );
    });
    let client = Client::connect(
        Config::new(address)
            .require_capabilities(CAPABILITY_STREAMED_QUERY_RESULTS)
            .require_schema(required),
    )
    .unwrap();
    assert_eq!(client.server_info().protocol_version, PROTOCOL_VERSION);
    assert_eq!(client.server_info().tables.len(), 2);
    drop(client);
    join.join().unwrap();

    for (name, required_schemas, required_capabilities) in [
        (
            "missing",
            vec![TableIdentity {
                table_id: TableId(9),
                fingerprint: SchemaFingerprint::from_bytes([9; 32]),
            }],
            0,
        ),
        (
            "mismatch",
            vec![TableIdentity {
                table_id: TableId(1),
                fingerprint: SchemaFingerprint::from_bytes([9; 32]),
            }],
            0,
        ),
        ("capability", Vec::new(), 1_u64 << 63),
    ] {
        let (address, join) = scripted_server(|mut stream| {
            hello(
                &mut stream,
                SERVER_CAPABILITIES,
                vec![TableSchemaIdentity {
                    table_id: TableId(1),
                    fingerprint: [1; 32],
                }],
            );
        });
        let mut config = Config::new(address).require_capabilities(required_capabilities);
        for identity in required_schemas {
            config = config.require_schema(identity);
        }
        let error = match Client::connect(config) {
            Ok(_) => panic!("{name} gate unexpectedly succeeded"),
            Err(error) => error,
        };
        match name {
            "missing" => assert!(matches!(error, ClientError::SchemaUnavailable { .. })),
            "mismatch" => assert!(matches!(error, ClientError::SchemaMismatch { .. })),
            "capability" => assert!(matches!(error, ClientError::CapabilityMismatch { .. })),
            _ => unreachable!(),
        }
        join.join().unwrap();
    }
}

#[test]
fn duplicate_hello_table_poisoning_rejects_connect() {
    let (address, join) = scripted_server(|mut stream| {
        hello(
            &mut stream,
            SERVER_CAPABILITIES,
            vec![
                TableSchemaIdentity {
                    table_id: TableId(1),
                    fingerprint: [1; 32],
                },
                TableSchemaIdentity {
                    table_id: TableId(1),
                    fingerprint: [2; 32],
                },
            ],
        );
    });
    assert!(matches!(
        Client::connect(Config::new(address)),
        Err(ClientError::Protocol { .. })
    ));
    join.join().unwrap();
}

#[test]
fn valid_server_error_keeps_connection_reusable() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let execute = read_request(&mut stream);
        write_response(
            &mut stream,
            execute.request_id,
            ServerMessage::Error {
                code: ProtocolErrorCode::Compile,
                transaction_state: WireTransactionState::None,
                message: "bad SQL".into(),
            },
        );
        let ping = read_request(&mut stream);
        assert_eq!(ping.message, ClientMessage::Ping);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
    });
    assert!(matches!(
        client.exec("not SQL"),
        Err(ClientError::Server(ServerError {
            code: ProtocolErrorCode::Compile,
            ..
        }))
    ));
    client.ping().unwrap();
    drop(client);
    join.join().unwrap();
}

#[test]
fn wrong_request_id_and_clean_eof_poison_the_connection() {
    for close_without_response in [false, true] {
        let (mut client, join) = connect_scripted(move |mut stream| {
            hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
            let request = read_request(&mut stream);
            if !close_without_response {
                write_response(&mut stream, request.request_id + 1, ServerMessage::Pong);
            }
        });
        assert!(matches!(client.ping(), Err(ClientError::Protocol { .. })));
        assert!(matches!(client.ping(), Err(ClientError::ConnectionClosed)));
        drop(client);
        join.join().unwrap();
    }
}

#[test]
fn rows_close_drains_and_drop_poisoning_prevents_reuse() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let query = read_request(&mut stream);
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::QueryStart {
                columns: query_columns(false),
            },
        );
        for value in [1, 2] {
            write_response(
                &mut stream,
                query.request_id,
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(value)],
                },
            );
        }
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::QueryEnd { row_count: 2 },
        );
        let ping = read_request(&mut stream);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
    });
    let mut rows = client.query("SELECT id FROM users").unwrap();
    assert_eq!(rows.columns()[0].name, "id");
    assert_eq!(rows.next_row().unwrap(), Some(vec![ScalarValue::Int64(1)]));
    rows.close().unwrap();
    client.ping().unwrap();
    drop(client);
    join.join().unwrap();

    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let query = read_request(&mut stream);
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::QueryStart {
                columns: query_columns(false),
            },
        );
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::QueryRow {
                values: vec![ScalarValue::Int64(1)],
            },
        );
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    {
        let mut rows = client.query("SELECT id FROM users").unwrap();
        assert!(rows.next_row().unwrap().is_some());
    }
    assert!(matches!(client.ping(), Err(ClientError::ConnectionClosed)));
    drop(client);
    join.join().unwrap();
}

#[test]
fn forgotten_rows_cannot_desynchronize_and_reuse_the_connection() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let query = read_request(&mut stream);
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::QueryStart {
                columns: query_columns(false),
            },
        );
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    let rows = client.query("SELECT id FROM users").unwrap();
    std::mem::forget(rows);
    assert!(matches!(
        client.ping(),
        Err(ClientError::ClientState {
            reason: "a query response is still open"
        })
    ));
    client.close().unwrap();
    join.join().unwrap();
}

#[test]
fn rows_reject_shape_type_nullability_and_count_mismatches() {
    let cases = [
        (
            "shape",
            query_columns(false),
            ServerMessage::QueryRow { values: vec![] },
        ),
        (
            "type",
            query_columns(false),
            ServerMessage::QueryRow {
                values: vec![ScalarValue::Text("wrong".into())],
            },
        ),
        (
            "nullability",
            query_columns(false),
            ServerMessage::QueryRow {
                values: vec![ScalarValue::Null],
            },
        ),
        (
            "count",
            query_columns(true),
            ServerMessage::QueryEnd { row_count: 1 },
        ),
    ];
    for (name, columns, invalid) in cases {
        let (mut client, join) = connect_scripted(move |mut stream| {
            hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
            let query = read_request(&mut stream);
            write_response(
                &mut stream,
                query.request_id,
                ServerMessage::QueryStart { columns },
            );
            write_response(&mut stream, query.request_id, invalid);
        });
        let mut rows = client.query("SELECT id FROM users").unwrap();
        assert!(
            matches!(rows.next_row(), Err(ClientError::Protocol { .. })),
            "{name} was accepted"
        );
        assert!(matches!(rows.close(), Err(ClientError::Protocol { .. })));
        assert!(matches!(client.ping(), Err(ClientError::ConnectionClosed)));
        drop(client);
        join.join().unwrap();
    }
}

#[test]
fn exec_drains_query_and_query_accepts_affected_rows_without_poisoning() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let execute = read_request(&mut stream);
        write_response(
            &mut stream,
            execute.request_id,
            ServerMessage::QueryStart {
                columns: query_columns(false),
            },
        );
        write_response(
            &mut stream,
            execute.request_id,
            ServerMessage::QueryRow {
                values: vec![ScalarValue::Int64(1)],
            },
        );
        write_response(
            &mut stream,
            execute.request_id,
            ServerMessage::QueryEnd { row_count: 1 },
        );
        let ping = read_request(&mut stream);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
        let query = read_request(&mut stream);
        write_response(
            &mut stream,
            query.request_id,
            ServerMessage::AffectedRows { count: 2 },
        );
        let ping = read_request(&mut stream);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
    });
    assert!(matches!(
        client.exec("SELECT id FROM users"),
        Err(ClientError::ExpectedAffectedRows)
    ));
    client.ping().unwrap();
    assert!(matches!(
        client.query("DELETE FROM users"),
        Err(ClientError::ExpectedQuery)
    ));
    client.ping().unwrap();
    drop(client);
    join.join().unwrap();
}

#[test]
fn transaction_wire_state_controls_terminal_lifecycle() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let begin = read_request(&mut stream);
        write_response(
            &mut stream,
            begin.request_id,
            ServerMessage::TransactionStarted,
        );
        let bad = read_request(&mut stream);
        write_response(
            &mut stream,
            bad.request_id,
            ServerMessage::Error {
                code: ProtocolErrorCode::Compile,
                transaction_state: WireTransactionState::Active,
                message: "bad SQL".into(),
            },
        );
        let ping = read_request(&mut stream);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
        let rolled_back = read_request(&mut stream);
        write_response(
            &mut stream,
            rolled_back.request_id,
            ServerMessage::Error {
                code: ProtocolErrorCode::Execution,
                transaction_state: WireTransactionState::None,
                message: "transaction rolled back".into(),
            },
        );
        let begin = read_request(&mut stream);
        write_response(
            &mut stream,
            begin.request_id,
            ServerMessage::TransactionStarted,
        );
        let rollback = read_request(&mut stream);
        write_response(
            &mut stream,
            rollback.request_id,
            ServerMessage::TransactionRolledBack,
        );
    });
    {
        let mut tx = client.begin(TableId(1)).unwrap();
        assert!(matches!(
            tx.exec("bad SQL"),
            Err(ClientError::Server(ServerError {
                transaction_state: WireTransactionState::Active,
                ..
            }))
        ));
        assert!(!tx.is_terminal());
        tx.ping().unwrap();
        assert!(matches!(tx.exec("fatal"), Err(ClientError::Server(_))));
        assert!(tx.is_terminal());
    }
    {
        let mut tx = client.begin(TableId(1)).unwrap();
        tx.rollback().unwrap();
        assert!(tx.is_terminal());
    }
    drop(client);
    join.join().unwrap();
}

#[test]
fn retryable_commit_error_retains_transaction_for_explicit_retry() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let begin = read_request(&mut stream);
        write_response(
            &mut stream,
            begin.request_id,
            ServerMessage::TransactionStarted,
        );
        let first_commit = read_request(&mut stream);
        assert_eq!(first_commit.message, ClientMessage::Commit);
        write_response(
            &mut stream,
            first_commit.request_id,
            ServerMessage::Error {
                code: ProtocolErrorCode::Storage,
                transaction_state: WireTransactionState::CommitPending,
                message: "retry commit".into(),
            },
        );
        let retry = read_request(&mut stream);
        assert_eq!(retry.message, ClientMessage::Commit);
        write_response(
            &mut stream,
            retry.request_id,
            ServerMessage::TransactionCommitted,
        );
        let ping = read_request(&mut stream);
        write_response(&mut stream, ping.request_id, ServerMessage::Pong);
    });
    {
        let mut tx = client.begin(TableId(1)).unwrap();
        assert!(matches!(
            tx.commit(),
            Err(ClientError::Server(ServerError {
                transaction_state: WireTransactionState::CommitPending,
                ..
            }))
        ));
        assert!(!tx.is_terminal());
        tx.commit().unwrap();
        assert!(tx.is_terminal());
    }
    client.ping().unwrap();
    drop(client);
    join.join().unwrap();
}

#[test]
fn dropping_active_transaction_closes_without_hidden_rollback_request() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let begin = read_request(&mut stream);
        write_response(
            &mut stream,
            begin.request_id,
            ServerMessage::TransactionStarted,
        );
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    {
        let tx = client.begin(TableId(1)).unwrap();
        drop(tx);
    }
    assert!(matches!(client.ping(), Err(ClientError::ConnectionClosed)));
    drop(client);
    join.join().unwrap();
}

#[test]
fn close_is_idempotent_and_request_id_exhaustion_is_checked() {
    let (mut client, join) = connect_scripted(|mut stream| {
        hello(&mut stream, SERVER_CAPABILITIES, Vec::new());
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    client.next_request_id = u64::MAX;
    assert!(matches!(
        client.ping(),
        Err(ClientError::RequestIdExhausted)
    ));
    client.close().unwrap();
    client.close().unwrap();
    join.join().unwrap();
}
