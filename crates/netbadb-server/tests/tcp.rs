use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};

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
    format!(
        r#"{{
            "version": 1,
            "listen": "127.0.0.1:0",
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
    std::fs::write(&manifest, manifest_json("users.ndb", "UserId")).unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    let server = TcpServer::new(config).start().unwrap();
    (directory, heap, table, server)
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

#[test]
fn malformed_truncated_and_zero_id_frames_are_connection_fatal_only() {
    let (directory, _heap, _table, server) = create_manifest_server("bad-frames");
    let address = server.local_addr();
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

    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn graceful_shutdown_closes_idle_sessions_rolls_back_and_closes_database() {
    let (directory, heap, table, server) = create_manifest_server("shutdown");
    let address = server.local_addr();
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
