use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use netbadb_core::Database;
use netbadb_protocol::{
    ClientMessage, Frame, ProtocolErrorCode, ServerMessage, WireTransactionState,
    encode_client_frame, read_server_frame, write_client_frame,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{ServerConfig, ServerHandle, TcpServer, TcpServerError};
use netbadb_storage::{wal_alternate_path, wal_path};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("netbadb-tcp-{name}-{}", std::process::id()))
}

fn cleanup(directory: &Path) {
    let _ = std::fs::remove_dir_all(directory);
}

fn users_table(semantic_name: &str) -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Semantic {
                    name: semantic_name.into(),
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    )
}

fn manifest_json(heap_name: &str, semantic_name: &str) -> String {
    manifest_json_with_limits(heap_name, semantic_name, None)
}

fn manifest_json_with_limits(heap_name: &str, semantic_name: &str, limits: Option<&str>) -> String {
    let limits = limits.map_or_else(String::new, |limits| format!("\"limits\": {limits},"));
    format!(
        r#"{{
            "version": 2,
            "listen": "127.0.0.1:0",
            {limits}
            "tables": [{{
                "path": "{heap_name}",
                "id": 1,
                "name": "users",
                "columns": [
                    {{
                        "id": 1,
                        "name": "id",
                        "physical_type": "int64",
                        "semantic_type": "{semantic_name}",
                        "nullable": false,
                        "primary_key": true
                    }},
                    {{
                        "id": 2,
                        "name": "name",
                        "physical_type": "text",
                        "semantic_type": null,
                        "nullable": true,
                        "primary_key": false
                    }}
                ]
            }}]
        }}"#
    )
}

fn create_manifest_server(name: &str) -> (PathBuf, PathBuf, TableDef, ServerHandle) {
    create_manifest_server_with_limits(name, None)
}

fn create_manifest_server_with_limits(
    name: &str,
    limits: Option<&str>,
) -> (PathBuf, PathBuf, TableDef, ServerHandle) {
    let directory = test_directory(name);
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    let table = users_table("UserId");
    Database::create(&heap, table.clone())
        .unwrap()
        .close()
        .unwrap();
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        manifest_json_with_limits("users.ndb", "UserId", limits),
    )
    .unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    let server = TcpServer::new(config).start().unwrap();
    (directory, heap, table, server)
}

fn wait_for_metrics(
    metrics: &netbadb_server::ServerMetricsHandle,
    predicate: impl Fn(netbadb_server::ServerMetricsSnapshot) -> bool,
) -> netbadb_server::ServerMetricsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = metrics.snapshot();
        if predicate(snapshot) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "metrics condition timed out: {snapshot:?}"
        );
        std::thread::yield_now();
    }
}

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(address: SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(address).unwrap(),
        }
    }

    fn request(&mut self, request_id: u64, message: ClientMessage) -> Vec<ServerMessage> {
        write_client_frame(
            &mut self.stream,
            &Frame {
                request_id,
                message,
            },
        )
        .unwrap();
        self.stream.flush().unwrap();

        let first = read_server_frame(&mut self.stream).unwrap().unwrap();
        assert_eq!(first.request_id, request_id);
        let is_query = matches!(first.message, ServerMessage::QueryStart { .. });
        let mut messages = vec![first.message];
        if is_query {
            loop {
                let frame = read_server_frame(&mut self.stream).unwrap().unwrap();
                assert_eq!(frame.request_id, request_id);
                let terminal = matches!(
                    frame.message,
                    ServerMessage::QueryEnd { .. } | ServerMessage::Error { .. }
                );
                messages.push(frame.message);
                if terminal {
                    break;
                }
            }
        }
        messages
    }

    fn hello(&mut self) -> Vec<ServerMessage> {
        self.request(1, ClientMessage::Hello)
    }

    fn close_clean(mut self) {
        self.stream.shutdown(Shutdown::Write).unwrap();
        assert!(read_server_frame(&mut self.stream).unwrap().is_none());
    }
}

fn assert_error(messages: &[ServerMessage], code: ProtocolErrorCode, state: WireTransactionState) {
    assert!(matches!(
        messages,
        [ServerMessage::Error {
            code: actual,
            transaction_state,
            ..
        }] if *actual == code && *transaction_state == state
    ));
}

#[test]
fn manifest_bootstrap_serves_handshake_query_dml_and_disconnect_rollback() {
    let (directory, _heap, table, server) = create_manifest_server("vertical");
    let address = server.local_addr();
    let mut client = Client::connect(address);
    assert!(matches!(
        client.hello().as_slice(),
        [ServerMessage::HelloAck { tables, .. }]
            if tables.len() == 1
                && tables[0].table_id == table.id
                && tables[0].fingerprint == *table.fingerprint().unwrap().as_bytes()
    ));
    assert_eq!(
        client.request(
            2,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (1, 'persisted')".into(),
            },
        ),
        vec![ServerMessage::AffectedRows { count: 1 }]
    );
    let query = client.request(
        3,
        ClientMessage::Execute {
            sql: "SELECT id, name FROM users ORDER BY id".into(),
        },
    );
    assert!(matches!(
        query.as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryRow { values },
            ServerMessage::QueryEnd { row_count: 1 }
        ] if values == &vec![ScalarValue::Int64(1), ScalarValue::Text("persisted".into())]
    ));

    assert_eq!(
        client.request(
            4,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        ),
        vec![ServerMessage::TransactionStarted]
    );
    assert_eq!(
        client.request(
            5,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (2, 'temporary')".into(),
            },
        ),
        vec![ServerMessage::AffectedRows { count: 1 }]
    );
    let own_write = client.request(
        6,
        ClientMessage::Execute {
            sql: "SELECT id FROM users ORDER BY id".into(),
        },
    );
    assert!(own_write.iter().any(|message| matches!(
        message,
        ServerMessage::QueryRow { values } if values == &vec![ScalarValue::Int64(2)]
    )));
    client.close_clean();

    let mut after_disconnect = Client::connect(address);
    after_disconnect.hello();
    let absent = after_disconnect.request(
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM users ORDER BY id".into(),
        },
    );
    assert!(matches!(
        absent.as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryRow { values },
            ServerMessage::QueryEnd { row_count: 1 }
        ] if values == &vec![ScalarValue::Int64(1)]
    ));
    after_disconnect.close_clean();

    let mut read_only = Client::connect(address);
    read_only.hello();
    assert_eq!(
        read_only.request(
            2,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        ),
        vec![ServerMessage::TransactionStarted]
    );
    read_only.close_clean();

    let mut after_read_only_disconnect = Client::connect(address);
    after_read_only_disconnect.hello();
    assert_eq!(
        after_read_only_disconnect.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    after_read_only_disconnect.close_clean();

    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn multiple_clients_keep_handshake_transaction_and_writer_state_isolated() {
    let (directory, _heap, _table, server) = create_manifest_server("sessions");
    let address = server.local_addr();
    let mut client_a = Client::connect(address);
    let mut client_b = Client::connect(address);
    client_a.hello();

    let before_hello = client_b.request(
        1,
        ClientMessage::Execute {
            sql: "SELECT id FROM users".into(),
        },
    );
    assert_error(
        &before_hello,
        ProtocolErrorCode::HandshakeRequired,
        WireTransactionState::None,
    );
    client_b.request(2, ClientMessage::Hello);
    assert_error(
        &client_b.request(3, ClientMessage::Commit),
        ProtocolErrorCode::NoActiveTransaction,
        WireTransactionState::None,
    );

    client_a.request(
        2,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );
    client_a.request(
        3,
        ClientMessage::Execute {
            sql: "INSERT INTO users (id, name) VALUES (10, 'owned')".into(),
        },
    );
    assert_eq!(
        client_b.request(4, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    assert_error(
        &client_b.request(
            5,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (20, 'blocked')".into(),
            },
        ),
        ProtocolErrorCode::Execution,
        WireTransactionState::None,
    );
    assert_eq!(
        client_a.request(4, ClientMessage::Rollback),
        vec![ServerMessage::TransactionRolledBack]
    );
    assert_eq!(
        client_b.request(
            6,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (20, 'retry')".into(),
            },
        ),
        vec![ServerMessage::AffectedRows { count: 1 }]
    );

    client_a.close_clean();
    client_b.close_clean();
    server.shutdown().unwrap();
    cleanup(&directory);
}

fn assert_bad_frame_closes_only_its_connection(address: SocketAddr, bytes: &[u8]) {
    let mut bad = TcpStream::connect(address).unwrap();
    bad.write_all(bytes).unwrap();
    bad.shutdown(Shutdown::Write).unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(bad.read(&mut byte).unwrap(), 0);

    let mut healthy = Client::connect(address);
    healthy.hello();
    assert_eq!(
        healthy.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    healthy.close_clean();
}

fn assert_connection_closes(mut stream: TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
}

#[test]
fn connection_limit_counts_unhandshaken_clients_and_updates_metrics() {
    let limits = r#"{
        "max_connections": 2,
        "idle_timeout_ms": 5000,
        "write_timeout_ms": 5000
    }"#;
    let (directory, _heap, _table, server) =
        create_manifest_server_with_limits("connection-limit", Some(limits));
    let address = server.local_addr();
    let metrics = server.metrics_handle();

    let client_a = TcpStream::connect(address).unwrap();
    let client_b = TcpStream::connect(address).unwrap();
    wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 2);

    let rejected = TcpStream::connect(address).unwrap();
    assert_connection_closes(rejected);
    let snapshot = wait_for_metrics(&metrics, |snapshot| {
        snapshot.rejected_connections_total == 1
    });
    assert_eq!(snapshot.accepted_connections_total, 2);

    client_a.shutdown(Shutdown::Both).unwrap();
    drop(client_a);
    wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 1);

    let mut replacement = Client::connect(address);
    replacement.hello();
    assert_eq!(
        replacement.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    replacement.close_clean();
    client_b.shutdown(Shutdown::Both).unwrap();
    drop(client_b);

    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.accepted_connections_total, 3);
    assert_eq!(snapshot.rejected_connections_total, 1);
    assert_eq!(snapshot.closed_connections_total, 3);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn idle_and_partial_frame_timeouts_close_connections_and_rollback_transactions() {
    let limits = r#"{
        "max_connections": 4,
        "idle_timeout_ms": 250,
        "write_timeout_ms": 5000
    }"#;
    let (directory, _heap, _table, server) =
        create_manifest_server_with_limits("idle-timeouts", Some(limits));
    let address = server.local_addr();
    let metrics = server.metrics_handle();

    assert_connection_closes(TcpStream::connect(address).unwrap());

    let mut partial = TcpStream::connect(address).unwrap();
    let hello = encode_client_frame(1, &ClientMessage::Hello).unwrap();
    partial.write_all(&hello[..10]).unwrap();
    partial.flush().unwrap();
    assert_connection_closes(partial);

    let mut transaction = Client::connect(address);
    transaction.hello();
    transaction.request(
        2,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );
    transaction.request(
        3,
        ClientMessage::Execute {
            sql: "INSERT INTO users (id, name) VALUES (9, 'timeout')".into(),
        },
    );
    assert!(
        read_server_frame(&mut transaction.stream)
            .unwrap()
            .is_none()
    );
    drop(transaction);

    let mut healthy = Client::connect(address);
    healthy.hello();
    let rows = healthy.request(
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM users".into(),
        },
    );
    assert!(matches!(
        rows.as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryEnd { row_count: 0 }
        ]
    ));
    healthy.close_clean();

    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.idle_timeouts_total, 3);
    assert_eq!(snapshot.protocol_failures_total, 0);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn result_row_limit_returns_one_error_without_poisoning_the_session() {
    let directory = test_directory("result-limit");
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    let table = users_table("UserId");
    let mut database = Database::create(&heap, table).unwrap();
    for id in 1..=3 {
        database
            .execute(&format!(
                "INSERT INTO users (id, name) VALUES ({id}, 'row')"
            ))
            .unwrap();
    }
    database.close().unwrap();
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        manifest_json_with_limits("users.ndb", "UserId", Some(r#"{"max_result_rows": 2}"#)),
    )
    .unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    let server = TcpServer::new(config).start().unwrap();
    let metrics = server.metrics_handle();
    let mut client = Client::connect(server.local_addr());
    client.hello();
    client.request(
        2,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );

    let limited = client.request(
        3,
        ClientMessage::Execute {
            sql: "SELECT id FROM users ORDER BY id".into(),
        },
    );
    assert_error(
        &limited,
        ProtocolErrorCode::ResponseTooLarge,
        WireTransactionState::Active,
    );
    assert_eq!(limited.len(), 1);
    assert_eq!(
        client.request(4, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    assert!(matches!(
        client
            .request(
                5,
                ClientMessage::Execute {
                    sql: "SELECT id FROM users ORDER BY id LIMIT 1".into(),
                },
            )
            .as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryRow { .. },
            ServerMessage::QueryEnd { row_count: 1 }
        ]
    ));
    assert_eq!(
        client.request(6, ClientMessage::Rollback),
        vec![ServerMessage::TransactionRolledBack]
    );
    client.close_clean();

    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.query_response_limit_errors_total, 1);
    assert_eq!(snapshot.worker_requests_total, 6);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn malformed_truncated_and_zero_id_frames_are_connection_fatal_only() {
    let (directory, _heap, _table, server) = create_manifest_server("bad-frames");
    let address = server.local_addr();
    let metrics = server.metrics_handle();
    let hello = encode_client_frame(1, &ClientMessage::Hello).unwrap();

    let mut bad_magic = hello.clone();
    bad_magic[0] = b'X';
    assert_bad_frame_closes_only_its_connection(address, &bad_magic);
    assert_bad_frame_closes_only_its_connection(address, &hello[..10]);
    let mut zero_id = hello;
    zero_id[16..24].fill(0);
    assert_bad_frame_closes_only_its_connection(address, &zero_id);

    let mut abandoned = Client::connect(address);
    abandoned.hello();
    write_client_frame(
        &mut abandoned.stream,
        &Frame {
            request_id: 2,
            message: ClientMessage::Execute {
                sql: "SELECT id, name FROM users".into(),
            },
        },
    )
    .unwrap();
    abandoned.stream.flush().unwrap();
    abandoned.stream.shutdown(Shutdown::Both).unwrap();
    drop(abandoned);

    let mut after_abandoned_response = Client::connect(address);
    after_abandoned_response.hello();
    assert_eq!(
        after_abandoned_response.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    after_abandoned_response.close_clean();

    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.protocol_failures_total == 3);
    assert_eq!(snapshot.idle_timeouts_total, 0);

    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn graceful_shutdown_closes_idle_sessions_rolls_back_and_closes_database() {
    let (directory, heap, table, server) = create_manifest_server("shutdown");
    let address = server.local_addr();
    let metrics = server.metrics_handle();
    let mut active = Client::connect(address);
    let mut idle = Client::connect(address);
    active.hello();
    idle.hello();
    active.request(
        2,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );
    active.request(
        3,
        ClientMessage::Execute {
            sql: "INSERT INTO users (id, name) VALUES (7, 'rollback')".into(),
        },
    );

    server.shutdown().unwrap();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.accepted_connections_total, 2);
    assert_eq!(snapshot.closed_connections_total, 2);
    assert_eq!(snapshot.active_connections, 0);
    drop(active);
    drop(idle);

    let mut reopened = Database::open(&heap, table).unwrap();
    assert!(
        reopened
            .query("SELECT id FROM users")
            .unwrap()
            .rows
            .is_empty()
    );
    reopened.close().unwrap();
    cleanup(&directory);
}

#[test]
fn startup_rejects_a_manifest_schema_that_disagrees_with_the_heap() {
    let directory = test_directory("schema-mismatch");
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    Database::create(&heap, users_table("UserId"))
        .unwrap()
        .close()
        .unwrap();
    let manifest = directory.join("server.json");
    std::fs::write(&manifest, manifest_json("users.ndb", "TeamId")).unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    assert!(matches!(
        TcpServer::new(config).start(),
        Err(TcpServerError::Database(_))
    ));

    let wal = wal_path(&heap);
    let _ = std::fs::remove_file(wal_alternate_path(&wal));
    cleanup(&directory);
}

#[test]
fn listener_bind_failure_waits_for_database_worker_cleanup() {
    let directory = test_directory("bind-failure");
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    let table = users_table("UserId");
    Database::create(&heap, table.clone())
        .unwrap()
        .close()
        .unwrap();

    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied_address = occupied.local_addr().unwrap();
    let manifest = directory.join("server.json");
    let source =
        manifest_json("users.ndb", "UserId").replace("127.0.0.1:0", &occupied_address.to_string());
    std::fs::write(&manifest, source).unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();

    assert!(matches!(
        TcpServer::new(config).start(),
        Err(TcpServerError::Bind { address, .. }) if address == occupied_address
    ));

    let reopened = Database::open(&heap, table).unwrap();
    reopened.close().unwrap();
    drop(occupied);
    cleanup(&directory);
}
