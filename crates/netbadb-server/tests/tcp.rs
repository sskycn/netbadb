use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use netbadb_core::Database;
use netbadb_protocol::{
    ClientMessage, Frame, ProtocolErrorCode, ServerMessage, WireTransactionState,
    encode_client_frame, read_server_frame, write_client_frame,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{
    AuthorizationConfigError, ManifestError, ServerConfig, ServerHandle, TcpServer, TcpServerError,
    TransportKind,
};
use netbadb_storage::{wal_alternate_path, wal_path};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use sha2::{Digest, Sha256};

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

fn teams_table() -> TableDef {
    TableDef::new(
        TableId(2),
        "teams",
        vec![
            ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Semantic {
                    name: "UserId".into(),
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn manifest_json(heap_name: &str, semantic_name: &str) -> String {
    manifest_json_with_limits(heap_name, semantic_name, None)
}

fn manifest_json_with_limits(heap_name: &str, semantic_name: &str, limits: Option<&str>) -> String {
    manifest_json_with_transport(
        heap_name,
        semantic_name,
        limits,
        None,
        &plaintext_authorization(),
    )
}

fn manifest_json_with_transport(
    heap_name: &str,
    semantic_name: &str,
    limits: Option<&str>,
    tls: Option<&str>,
    authorization: &str,
) -> String {
    let limits = limits.map_or_else(String::new, |limits| format!("\"limits\": {limits},"));
    let tls = tls.map_or_else(String::new, |tls| format!("\"tls\": {tls},"));
    format!(
        r#"{{
            "version": 4,
            "listen": "127.0.0.1:0",
            {limits}
            {tls}
            "authorization": {authorization},
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

fn table_permissions(read: bool, write: bool, transaction: bool, analyze: bool) -> String {
    format!(
        r#"{{"table_id":1,"read":{read},"write":{write},"transaction":{transaction},"analyze":{analyze}}}"#
    )
}

fn plaintext_authorization() -> String {
    format!(
        r#"{{"local_plaintext":{{"tables":[{}]}},"clients":[]}}"#,
        table_permissions(true, true, true, true)
    )
}

fn certificate_fingerprint(certificate: &[u8]) -> String {
    Sha256::digest(certificate)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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

fn create_two_table_server(name: &str, authorization: &str) -> (PathBuf, ServerHandle) {
    let directory = test_directory(name);
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let users = users_table("UserId");
    let teams = teams_table();
    Database::create_tables(vec![
        (directory.join("users.ndb"), users),
        (directory.join("teams.ndb"), teams),
    ])
    .unwrap()
    .close()
    .unwrap();
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        format!(
            r#"{{
                "version":4,
                "listen":"127.0.0.1:0",
                "authorization":{authorization},
                "tables":[
                    {{
                        "path":"users.ndb","id":1,"name":"users",
                        "columns":[
                            {{"id":1,"name":"id","physical_type":"int64","semantic_type":"UserId","nullable":false,"primary_key":true}},
                            {{"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":true,"primary_key":false}}
                        ]
                    }},
                    {{
                        "path":"teams.ndb","id":2,"name":"teams",
                        "columns":[
                            {{"id":1,"name":"id","physical_type":"int64","semantic_type":"UserId","nullable":false,"primary_key":true}},
                            {{"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":false,"primary_key":false}}
                        ]
                    }}
                ]
            }}"#
        ),
    )
    .unwrap();
    let server = TcpServer::new(ServerConfig::from_manifest_path(&manifest).unwrap())
        .start()
        .unwrap();
    (directory, server)
}

struct TestIdentity {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

struct TestPki {
    ca_certificate: Certificate,
    server_certificate: Certificate,
    server_private_key: KeyPair,
    valid_client: TestIdentity,
    reader_client: TestIdentity,
    unlisted_client: TestIdentity,
    untrusted_client: TestIdentity,
}

impl TestPki {
    fn generate() -> Self {
        let (ca_certificate, ca_private_key) = generate_ca();
        let (server_certificate, server_private_key) =
            generate_leaf(&ca_certificate, &ca_private_key, true);
        let (valid_client_certificate, valid_client_private_key) =
            generate_leaf(&ca_certificate, &ca_private_key, false);
        let (reader_client_certificate, reader_client_private_key) =
            generate_leaf(&ca_certificate, &ca_private_key, false);
        let (unlisted_client_certificate, unlisted_client_private_key) =
            generate_leaf(&ca_certificate, &ca_private_key, false);
        let (untrusted_ca, untrusted_ca_key) = generate_ca();
        let (untrusted_client_certificate, untrusted_client_private_key) =
            generate_leaf(&untrusted_ca, &untrusted_ca_key, false);
        Self {
            ca_certificate,
            server_certificate,
            server_private_key,
            valid_client: TestIdentity {
                certificate: valid_client_certificate.der().to_vec(),
                private_key: valid_client_private_key.serialize_der(),
            },
            reader_client: TestIdentity {
                certificate: reader_client_certificate.der().to_vec(),
                private_key: reader_client_private_key.serialize_der(),
            },
            unlisted_client: TestIdentity {
                certificate: unlisted_client_certificate.der().to_vec(),
                private_key: unlisted_client_private_key.serialize_der(),
            },
            untrusted_client: TestIdentity {
                certificate: untrusted_client_certificate.der().to_vec(),
                private_key: untrusted_client_private_key.serialize_der(),
            },
        }
    }

    fn authorization_json(&self) -> String {
        format!(
            r#"{{"clients":[{{"certificate_sha256":"{}","tables":[{}]}},{{"certificate_sha256":"{}","tables":[{}]}}]}}"#,
            certificate_fingerprint(&self.valid_client.certificate),
            table_permissions(true, true, true, true),
            certificate_fingerprint(&self.reader_client.certificate),
            table_permissions(true, false, false, false),
        )
    }

    fn write_server_material(&self, directory: &Path) -> String {
        std::fs::write(directory.join("server.pem"), self.server_certificate.pem()).unwrap();
        std::fs::write(
            directory.join("server-key.pem"),
            self.server_private_key.serialize_pem(),
        )
        .unwrap();
        std::fs::write(directory.join("client-ca.pem"), self.ca_certificate.pem()).unwrap();
        r#"{
            "server_certificate": "server.pem",
            "server_private_key": "server-key.pem",
            "client_ca": "client-ca.pem"
        }"#
        .into()
    }

    fn client_config(&self, identity: Option<&TestIdentity>) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(self.ca_certificate.der().to_vec()))
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        let config = match identity {
            Some(identity) => builder
                .with_client_auth_cert(
                    vec![CertificateDer::from(identity.certificate.clone())],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.private_key.clone())),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        Arc::new(config)
    }
}

fn generate_ca() -> (Certificate, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let certificate = params.self_signed(&key).unwrap();
    (certificate, key)
}

fn generate_leaf(
    issuer: &Certificate,
    issuer_key: &KeyPair,
    server: bool,
) -> (Certificate, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let names = if server {
        vec!["localhost".into(), "127.0.0.1".into()]
    } else {
        Vec::new()
    };
    let mut params = CertificateParams::new(names).unwrap();
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![if server {
        ExtendedKeyUsagePurpose::ServerAuth
    } else {
        ExtendedKeyUsagePurpose::ClientAuth
    }];
    let certificate = params.signed_by(&key, issuer, issuer_key).unwrap();
    (certificate, key)
}

fn create_manifest_tls_server_with_limits(
    name: &str,
    limits: Option<&str>,
) -> (PathBuf, PathBuf, TableDef, TestPki, ServerHandle) {
    let directory = test_directory(name);
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    let table = users_table("UserId");
    Database::create(&heap, table.clone())
        .unwrap()
        .close()
        .unwrap();
    let pki = TestPki::generate();
    let tls = pki.write_server_material(&directory);
    let authorization = pki.authorization_json();
    let manifest = directory.join("server.json");
    std::fs::write(
        &manifest,
        manifest_json_with_transport("users.ndb", "UserId", limits, Some(&tls), &authorization),
    )
    .unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    assert_eq!(config.transport_kind(), TransportKind::MutualTls);
    let server = TcpServer::new(config).start().unwrap();
    assert_eq!(server.transport_kind(), TransportKind::MutualTls);
    (directory, heap, table, pki, server)
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
        request(&mut self.stream, request_id, message)
    }

    fn hello(&mut self) -> Vec<ServerMessage> {
        self.request(1, ClientMessage::Hello)
    }

    fn close_clean(mut self) {
        self.stream.shutdown(Shutdown::Write).unwrap();
        assert!(read_server_frame(&mut self.stream).unwrap().is_none());
    }
}

struct TlsClient {
    stream: StreamOwned<ClientConnection, TcpStream>,
}

impl TlsClient {
    fn connect(address: SocketAddr, config: Arc<ClientConfig>) -> std::io::Result<Self> {
        let mut socket = TcpStream::connect(address)?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        socket.set_write_timeout(Some(Duration::from_secs(5)))?;
        let server_name = ServerName::try_from("localhost").unwrap().to_owned();
        let mut connection = ClientConnection::new(config, server_name)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        while connection.is_handshaking() {
            connection.complete_io(&mut socket)?;
        }
        Ok(Self {
            stream: StreamOwned::new(connection, socket),
        })
    }

    fn request(&mut self, request_id: u64, message: ClientMessage) -> Vec<ServerMessage> {
        request(&mut self.stream, request_id, message)
    }

    fn hello(&mut self) -> Vec<ServerMessage> {
        self.request(1, ClientMessage::Hello)
    }

    fn close_clean(mut self) {
        self.stream.conn.send_close_notify();
        self.stream.flush().unwrap();
        self.stream.get_ref().shutdown(Shutdown::Write).unwrap();
        assert!(read_server_frame(&mut self.stream).unwrap().is_none());
    }

    fn disconnect(self) {
        self.stream.get_ref().shutdown(Shutdown::Both).unwrap();
    }
}

fn request(
    stream: &mut (impl Read + Write),
    request_id: u64,
    message: ClientMessage,
) -> Vec<ServerMessage> {
    write_client_frame(
        &mut *stream,
        &Frame {
            request_id,
            message,
        },
    )
    .unwrap();
    stream.flush().unwrap();

    let first = read_server_frame(&mut *stream).unwrap().unwrap();
    assert_eq!(first.request_id, request_id);
    let is_query = matches!(first.message, ServerMessage::QueryStart { .. });
    let mut messages = vec![first.message];
    if is_query {
        loop {
            let frame = read_server_frame(&mut *stream).unwrap().unwrap();
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
fn plaintext_authorization_filters_hello_and_preflights_cross_table_access() {
    let restricted = format!(
        r#"{{"local_plaintext":{{"tables":[{}]}},"clients":[]}}"#,
        table_permissions(true, false, true, false)
    );
    let (directory, server) = create_two_table_server("authorization-read", &restricted);
    let metrics = server.metrics_handle();
    let mut client = Client::connect(server.local_addr());
    assert_error(
        &client.request(
            1,
            ClientMessage::Execute {
                sql: "SELECT id FROM teams".into(),
            },
        ),
        ProtocolErrorCode::HandshakeRequired,
        WireTransactionState::None,
    );
    assert!(matches!(
        client.hello().as_slice(),
        [ServerMessage::HelloAck { tables, .. }]
            if tables.iter().map(|table| table.table_id).collect::<Vec<_>>() == vec![TableId(1)]
    ));
    assert!(matches!(
        client
            .request(
                2,
                ClientMessage::Execute {
                    sql: "SELECT u.id FROM users u".into(),
                },
            )
            .as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryEnd { row_count: 0 }
        ]
    ));
    let denied_join = client.request(
        3,
        ClientMessage::Execute {
            sql: "SELECT u.id FROM users u JOIN teams t ON u.id = t.id".into(),
        },
    );
    assert_error(
        &denied_join,
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    assert_eq!(
        client.request(
            4,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        ),
        vec![ServerMessage::TransactionStarted]
    );
    let denied_in_transaction = client.request(
        5,
        ClientMessage::Execute {
            sql: "SELECT t.id FROM teams t".into(),
        },
    );
    assert_error(
        &denied_in_transaction,
        ProtocolErrorCode::Database,
        WireTransactionState::Active,
    );
    assert_error(
        &client.request(
            6,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (1, 'denied')".into(),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::Active,
    );
    assert!(matches!(
        client
            .request(
                7,
                ClientMessage::Execute {
                    sql: "SELECT id FROM users".into(),
                },
            )
            .as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryEnd { row_count: 0 }
        ]
    ));
    assert_eq!(
        client.request(8, ClientMessage::Rollback),
        vec![ServerMessage::TransactionRolledBack]
    );
    assert_error(
        &client.request(
            9,
            ClientMessage::Analyze {
                table_id: TableId(1),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    assert_error(
        &client.request(
            10,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (1, 'denied')".into(),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    assert_error(
        &client.request(
            11,
            ClientMessage::Execute {
                sql: "SELECT FROM".into(),
            },
        ),
        ProtocolErrorCode::Compile,
        WireTransactionState::None,
    );
    assert_eq!(
        client.request(12, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    client.close_clean();
    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.authorization_denials_total, 5);
    server.shutdown().unwrap();

    let full_read = format!(
        r#"{{"local_plaintext":{{"tables":[{},{}]}},"clients":[]}}"#,
        table_permissions(true, false, false, false),
        r#"{"table_id":2,"read":true}"#,
    );
    let manifest = directory.join("server.json");
    let source = std::fs::read_to_string(&manifest)
        .unwrap()
        .replace(&restricted, &full_read);
    std::fs::write(&manifest, source).unwrap();
    let server = TcpServer::new(ServerConfig::from_manifest_path(&manifest).unwrap())
        .start()
        .unwrap();
    let mut client = Client::connect(server.local_addr());
    assert!(matches!(
        client.hello().as_slice(),
        [ServerMessage::HelloAck { tables, .. }]
            if tables.iter().map(|table| table.table_id).collect::<Vec<_>>()
                == vec![TableId(1), TableId(2)]
    ));
    assert!(matches!(
        client
            .request(
                2,
                ClientMessage::Execute {
                    sql: "SELECT u.id FROM users u JOIN teams t ON u.id = t.id".into(),
                },
            )
            .as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryEnd { row_count: 0 }
        ]
    ));
    client.close_clean();
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn write_only_plaintext_principal_keeps_write_transaction_and_analyze_independent() {
    let write_only = format!(
        r#"{{"local_plaintext":{{"tables":[{}]}},"clients":[]}}"#,
        table_permissions(false, true, false, false)
    );
    let (directory, server) = create_two_table_server("authorization-write", &write_only);
    let metrics = server.metrics_handle();
    let mut client = Client::connect(server.local_addr());
    client.hello();
    for (request_id, sql) in [
        (2, "INSERT INTO users (id, name) VALUES (1, 'created')"),
        (
            3,
            "UPDATE users SET name = 'updated' WHERE name = 'created'",
        ),
        (4, "DELETE FROM users WHERE name = 'updated'"),
    ] {
        assert_eq!(
            client.request(request_id, ClientMessage::Execute { sql: sql.into() },),
            vec![ServerMessage::AffectedRows { count: 1 }]
        );
    }
    assert_error(
        &client.request(
            5,
            ClientMessage::Execute {
                sql: "SELECT id FROM users".into(),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    assert_error(
        &client.request(
            6,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    assert_error(
        &client.request(
            7,
            ClientMessage::Analyze {
                table_id: TableId(1),
            },
        ),
        ProtocolErrorCode::Database,
        WireTransactionState::None,
    );
    client.close_clean();
    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.authorization_denials_total, 3);
    server.shutdown().unwrap();

    let mut database = Database::open(directory.join("users.ndb"), users_table("UserId")).unwrap();
    assert!(
        database
            .query("SELECT id FROM users")
            .unwrap()
            .rows
            .is_empty()
    );
    database.close().unwrap();
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

fn assert_tls_handshake_rejected(address: SocketAddr, config: Arc<ClientConfig>) {
    match TlsClient::connect(address, config) {
        Err(_) => {}
        Ok(mut client) => {
            let mut byte = [0_u8; 1];
            assert!(matches!(client.stream.read(&mut byte), Ok(0) | Err(_)));
        }
    }
}

#[test]
fn manifest_v4_validates_tls_material_and_allows_secure_remote_configuration() {
    let directory = test_directory("tls-manifest");
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let heap = directory.join("users.ndb");
    Database::create(&heap, users_table("UserId"))
        .unwrap()
        .close()
        .unwrap();
    let pki = TestPki::generate();
    let tls = pki.write_server_material(&directory);
    let authorization = pki.authorization_json();
    let manifest = directory.join("server.json");

    let secure_remote =
        manifest_json_with_transport("users.ndb", "UserId", None, Some(&tls), &authorization)
            .replace("127.0.0.1:0", "192.0.2.1:7878");
    std::fs::write(&manifest, &secure_remote).unwrap();
    let config = ServerConfig::from_manifest_path(&manifest).unwrap();
    assert_eq!(config.transport_kind(), TransportKind::MutualTls);

    for (invalid_authorization, expected) in [
        (
            r#"{"clients":[]}"#.to_owned(),
            AuthorizationConfigError::MutualTlsClientsRequired,
        ),
        (
            format!(
                r#"{{"local_plaintext":{{"tables":[{}]}},"clients":[{{"certificate_sha256":"{}","tables":[{}]}}]}}"#,
                table_permissions(true, false, false, false),
                certificate_fingerprint(&pki.valid_client.certificate),
                table_permissions(true, false, false, false),
            ),
            AuthorizationConfigError::MutualTlsLocalPolicyNotAllowed,
        ),
        (
            format!(
                r#"{{"clients":[{{"certificate_sha256":"{0}","tables":[{1}]}},{{"certificate_sha256":"{0}","tables":[{1}]}}]}}"#,
                certificate_fingerprint(&pki.valid_client.certificate),
                table_permissions(true, false, false, false),
            ),
            AuthorizationConfigError::DuplicateClientFingerprint,
        ),
    ] {
        std::fs::write(
            &manifest,
            manifest_json_with_transport(
                "users.ndb",
                "UserId",
                None,
                Some(&tls),
                &invalid_authorization,
            ),
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Authorization(actual)) if actual == expected
        ));
    }
    std::fs::write(&manifest, &secure_remote).unwrap();

    let missing_field = manifest_json_with_transport(
        "users.ndb",
        "UserId",
        None,
        Some(
            r#"{
                "server_certificate": "server.pem",
                "server_private_key": "server-key.pem"
            }"#,
        ),
        &authorization,
    );
    std::fs::write(&manifest, missing_field).unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());

    let unknown_field = tls.replace("\"client_ca\"", "\"allow_anonymous\": true, \"client_ca\"");
    std::fs::write(
        &manifest,
        manifest_json_with_transport(
            "users.ndb",
            "UserId",
            None,
            Some(&unknown_field),
            &authorization,
        ),
    )
    .unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());

    std::fs::write(directory.join("server.pem"), "").unwrap();
    std::fs::write(
        &manifest,
        manifest_json_with_transport("users.ndb", "UserId", None, Some(&tls), &authorization),
    )
    .unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());
    std::fs::write(directory.join("server.pem"), pki.server_certificate.pem()).unwrap();

    std::fs::write(directory.join("server-key.pem"), "not a private key").unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());

    let unrelated_key = KeyPair::generate().unwrap();
    std::fs::write(
        directory.join("server-key.pem"),
        unrelated_key.serialize_pem(),
    )
    .unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());
    std::fs::write(
        directory.join("server-key.pem"),
        format!(
            "{}{}",
            pki.server_private_key.serialize_pem(),
            unrelated_key.serialize_pem()
        ),
    )
    .unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());
    std::fs::write(
        directory.join("server-key.pem"),
        pki.server_private_key.serialize_pem(),
    )
    .unwrap();

    std::fs::write(directory.join("client-ca.pem"), "").unwrap();
    assert!(ServerConfig::from_manifest_path(&manifest).is_err());
    cleanup(&directory);
}

#[test]
fn mutual_tls_serves_protocol_and_preserves_result_policy() {
    let limits = r#"{
        "max_result_rows": 2,
        "idle_timeout_ms": 5000,
        "write_timeout_ms": 5000
    }"#;
    let (directory, _heap, table, pki, server) =
        create_manifest_tls_server_with_limits("tls-vertical", Some(limits));
    let metrics = server.metrics_handle();
    let mut client = TlsClient::connect(
        server.local_addr(),
        pki.client_config(Some(&pki.valid_client)),
    )
    .unwrap();

    assert!(matches!(
        client.hello().as_slice(),
        [ServerMessage::HelloAck { tables, .. }]
            if tables.len() == 1
                && tables[0].table_id == table.id
                && tables[0].fingerprint == *table.fingerprint().unwrap().as_bytes()
    ));
    assert_eq!(
        client.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    for id in 1..=3 {
        assert_eq!(
            client.request(
                id + 2,
                ClientMessage::Execute {
                    sql: format!("INSERT INTO users (id, name) VALUES ({id}, 'tls')"),
                },
            ),
            vec![ServerMessage::AffectedRows { count: 1 }]
        );
    }
    assert_eq!(
        client.request(
            6,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        ),
        vec![ServerMessage::TransactionStarted]
    );

    let mut reader = TlsClient::connect(
        server.local_addr(),
        pki.client_config(Some(&pki.reader_client)),
    )
    .unwrap();
    reader.hello();
    assert!(matches!(
        reader
            .request(
                2,
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
    for (request_id, sql) in [
        (3, "INSERT INTO users (id, name) VALUES (9, 'denied')"),
        (4, "UPDATE users SET name = 'denied' WHERE name = 'tls'"),
        (5, "DELETE FROM users WHERE name = 'tls'"),
    ] {
        let denied = reader.request(request_id, ClientMessage::Execute { sql: sql.into() });
        assert_error(
            &denied,
            ProtocolErrorCode::Database,
            WireTransactionState::None,
        );
        assert!(matches!(
            denied.as_slice(),
            [ServerMessage::Error { message, .. }] if message.starts_with("authorization denied")
        ));
    }
    assert_eq!(
        reader.request(6, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    reader.close_clean();

    assert_eq!(
        client.request(
            7,
            ClientMessage::Execute {
                sql: "UPDATE users SET name = 'committed' WHERE name = 'tls'".into(),
            },
        ),
        vec![ServerMessage::AffectedRows { count: 3 }]
    );
    assert_eq!(
        client.request(8, ClientMessage::Commit),
        vec![ServerMessage::TransactionCommitted]
    );
    let limited = client.request(
        9,
        ClientMessage::Execute {
            sql: "SELECT id FROM users ORDER BY id".into(),
        },
    );
    assert_error(
        &limited,
        ProtocolErrorCode::ResponseTooLarge,
        WireTransactionState::None,
    );
    assert_eq!(
        client.request(10, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    client.close_clean();

    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.tls_handshakes_total, 2);
    assert_eq!(snapshot.tls_handshake_failures_total, 0);
    assert_eq!(snapshot.authenticated_connections_total, 2);
    assert_eq!(snapshot.authorization_denials_total, 3);
    assert_eq!(snapshot.query_response_limit_errors_total, 1);
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn mutual_tls_rejects_unauthenticated_untrusted_and_plaintext_clients() {
    let (directory, _heap, _table, pki, server) =
        create_manifest_tls_server_with_limits("tls-auth-failures", None);
    let address = server.local_addr();
    let metrics = server.metrics_handle();

    assert_tls_handshake_rejected(address, pki.client_config(None));
    assert_tls_handshake_rejected(address, pki.client_config(Some(&pki.untrusted_client)));

    let mut plaintext = TcpStream::connect(address).unwrap();
    plaintext
        .write_all(&encode_client_frame(1, &ClientMessage::Hello).unwrap())
        .unwrap();
    plaintext.shutdown(Shutdown::Write).unwrap();
    plaintext
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut response = Vec::new();
    let _ = plaintext.read_to_end(&mut response);
    if let Ok(Some(frame)) = read_server_frame(&mut response.as_slice()) {
        assert!(!matches!(frame.message, ServerMessage::Error { .. }));
    }

    let failures = wait_for_metrics(&metrics, |snapshot| {
        snapshot.tls_handshake_failures_total == 3 && snapshot.active_connections == 0
    });
    assert_eq!(failures.tls_handshakes_total, 0);
    assert_eq!(failures.authenticated_connections_total, 0);
    assert_eq!(failures.protocol_failures_total, 0);

    let mut unlisted =
        TlsClient::connect(address, pki.client_config(Some(&pki.unlisted_client))).unwrap();
    let mut byte = [0_u8; 1];
    match unlisted.stream.read(&mut byte) {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
        result => panic!("unlisted client connection remained usable: {result:?}"),
    }
    let admission_denied = wait_for_metrics(&metrics, |snapshot| {
        snapshot.tls_handshakes_total == 1
            && snapshot.authorization_denials_total == 1
            && snapshot.active_connections == 0
    });
    assert_eq!(admission_denied.authenticated_connections_total, 0);
    assert_eq!(admission_denied.tls_handshake_failures_total, 3);

    let mut healthy =
        TlsClient::connect(address, pki.client_config(Some(&pki.valid_client))).unwrap();
    healthy.hello();
    assert_eq!(
        healthy.request(2, ClientMessage::Ping),
        vec![ServerMessage::Pong]
    );
    healthy.close_clean();
    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.tls_handshakes_total, 2);
    assert_eq!(snapshot.tls_handshake_failures_total, 3);
    assert_eq!(snapshot.authenticated_connections_total, 1);
    assert_eq!(snapshot.authorization_denials_total, 1);

    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn mutual_tls_disconnect_rolls_back_active_transaction() {
    let (directory, _heap, _table, pki, server) =
        create_manifest_tls_server_with_limits("tls-rollback", None);
    let config = pki.client_config(Some(&pki.valid_client));
    let mut writer = TlsClient::connect(server.local_addr(), Arc::clone(&config)).unwrap();
    writer.hello();
    writer.request(
        2,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );
    writer.request(
        3,
        ClientMessage::Execute {
            sql: "INSERT INTO users (id, name) VALUES (9, 'rollback')".into(),
        },
    );
    writer.disconnect();

    let metrics = server.metrics_handle();
    wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    let mut reader = TlsClient::connect(server.local_addr(), config).unwrap();
    reader.hello();
    assert!(matches!(
        reader
            .request(
                2,
                ClientMessage::Execute {
                    sql: "SELECT id FROM users".into(),
                },
            )
            .as_slice(),
        [
            ServerMessage::QueryStart { .. },
            ServerMessage::QueryEnd { row_count: 0 }
        ]
    ));
    reader.close_clean();
    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn tls_handshake_timeout_connection_cap_and_shutdown_are_bounded() {
    let limits = r#"{
        "max_connections": 2,
        "idle_timeout_ms": 5000,
        "write_timeout_ms": 5000
    }"#;
    let (directory, _heap, _table, _pki, server) =
        create_manifest_tls_server_with_limits("tls-cap-shutdown", Some(limits));
    let address = server.local_addr();
    let metrics = server.metrics_handle();
    let pending_a = TcpStream::connect(address).unwrap();
    let pending_b = TcpStream::connect(address).unwrap();
    wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 2);
    assert_connection_closes(TcpStream::connect(address).unwrap());
    assert_eq!(
        wait_for_metrics(&metrics, |snapshot| snapshot.rejected_connections_total
            == 1)
        .accepted_connections_total,
        2
    );

    let started = Instant::now();
    server.shutdown().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(metrics.snapshot().active_connections, 0);
    drop(pending_a);
    drop(pending_b);
    cleanup(&directory);

    let timeout_limits = r#"{
        "max_connections": 1,
        "idle_timeout_ms": 200,
        "write_timeout_ms": 5000
    }"#;
    let (directory, _heap, _table, _pki, server) =
        create_manifest_tls_server_with_limits("tls-timeout", Some(timeout_limits));
    let metrics = server.metrics_handle();
    assert_connection_closes(TcpStream::connect(server.local_addr()).unwrap());
    let snapshot = wait_for_metrics(&metrics, |snapshot| snapshot.active_connections == 0);
    assert_eq!(snapshot.tls_handshake_failures_total, 1);
    assert_eq!(snapshot.idle_timeouts_total, 1);
    assert_eq!(snapshot.authenticated_connections_total, 0);
    server.shutdown().unwrap();
    cleanup(&directory);
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
