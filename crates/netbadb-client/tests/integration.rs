use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use netbadb_client::{Client, ClientError, Config, TableIdentity, TlsConfig, TlsConfigError};
use netbadb_core::Database;
use netbadb_protocol::{CAPABILITY_EXPLICIT_TRANSACTIONS, CAPABILITY_STREAMED_QUERY_RESULTS};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{ServerConfig, ServerHandle, TcpServer};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sha2::{Digest, Sha256};

fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("netbadb-rust-client-{name}-{}", std::process::id()))
}

fn cleanup(directory: &Path) {
    let _ = std::fs::remove_dir_all(directory);
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
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    )
}

fn table_permissions() -> &'static str {
    r#"{"table_id":1,"read":true,"write":true,"transaction":true,"analyze":true}"#
}

fn plaintext_manifest() -> String {
    format!(
        r#"{{
            "version":4,
            "listen":"127.0.0.1:0",
            "authorization":{{"local_plaintext":{{"tables":[{}]}},"clients":[]}},
            "tables":[{{
                "path":"users.ndb","id":1,"name":"users",
                "columns":[
                    {{"id":1,"name":"id","physical_type":"int64","semantic_type":"UserId","nullable":false,"primary_key":true}},
                    {{"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":true,"primary_key":false}}
                ]
            }}]
        }}"#,
        table_permissions()
    )
}

fn create_database(directory: &Path) -> TableDef {
    cleanup(directory);
    std::fs::create_dir_all(directory).unwrap();
    let table = users_table();
    Database::create(directory.join("users.ndb"), table.clone())
        .unwrap()
        .close()
        .unwrap();
    table
}

fn start_server(directory: &Path, manifest: String) -> ServerHandle {
    let manifest_path = directory.join("server.json");
    std::fs::write(&manifest_path, manifest).unwrap();
    TcpServer::new(ServerConfig::from_manifest_path(manifest_path).unwrap())
        .start()
        .unwrap()
}

fn client_config(server: &ServerHandle, table: &TableDef) -> Config {
    Config::new(server.local_addr().to_string())
        .require_capabilities(CAPABILITY_EXPLICIT_TRANSACTIONS | CAPABILITY_STREAMED_QUERY_RESULTS)
        .require_schema(TableIdentity::from_table(table).unwrap())
        .read_timeout(Some(Duration::from_secs(5)))
        .write_timeout(Some(Duration::from_secs(5)))
}

fn read_ids(client: &mut Client) -> Vec<i64> {
    let mut rows = client
        .query("SELECT id, name FROM users ORDER BY id")
        .unwrap();
    assert_eq!(rows.columns().len(), 2);
    assert_eq!(rows.columns()[0].data_type.name.as_deref(), Some("UserId"));
    let mut ids = Vec::new();
    while let Some(row) = rows.next_row().unwrap() {
        let ScalarValue::Int64(id) = row[0] else {
            panic!("server returned a non-INT64 id");
        };
        ids.push(id);
    }
    ids
}

fn wait_for_no_connections(server: &ServerHandle) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server.metrics().active_connections == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "connection cleanup timed out");
        std::thread::yield_now();
    }
}

#[test]
fn plaintext_remote_client_streams_and_preserves_transaction_cleanup() {
    let directory = test_directory("plaintext");
    let table = create_database(&directory);
    let server = start_server(&directory, plaintext_manifest());

    let mut client = Client::connect(client_config(&server, &table)).unwrap();
    client.ping().unwrap();
    client.analyze(table.id).unwrap();
    assert_eq!(
        client
            .exec("INSERT INTO users (id, name) VALUES (1, 'kept')")
            .unwrap(),
        1
    );
    assert_eq!(read_ids(&mut client), vec![1]);

    {
        let mut tx = client.begin(table.id).unwrap();
        assert_eq!(
            tx.exec("INSERT INTO users (id, name) VALUES (2, 'committed')")
                .unwrap(),
            1
        );
        tx.commit().unwrap();
        assert!(tx.is_terminal());
    }
    assert_eq!(read_ids(&mut client), vec![1, 2]);

    {
        let mut tx = client.begin(table.id).unwrap();
        assert_eq!(
            tx.exec("INSERT INTO users (id, name) VALUES (3, 'rolled back')")
                .unwrap(),
            1
        );
        let mut rows = tx.query("SELECT id FROM users ORDER BY id").unwrap();
        assert_eq!(rows.next_row().unwrap(), Some(vec![ScalarValue::Int64(1)]));
        assert_eq!(rows.next_row().unwrap(), Some(vec![ScalarValue::Int64(2)]));
        assert_eq!(rows.next_row().unwrap(), Some(vec![ScalarValue::Int64(3)]));
        rows.close().unwrap();
        tx.rollback().unwrap();
        assert!(tx.is_terminal());
    }
    assert_eq!(read_ids(&mut client), vec![1, 2]);

    {
        let mut tx = client.begin(table.id).unwrap();
        tx.exec("INSERT INTO users (id, name) VALUES (4, 'disconnect')")
            .unwrap();
    }
    assert!(matches!(client.ping(), Err(ClientError::ConnectionClosed)));
    drop(client);
    wait_for_no_connections(&server);

    let mut replacement = Client::connect(client_config(&server, &table)).unwrap();
    assert_eq!(read_ids(&mut replacement), vec![1, 2]);
    replacement.close().unwrap();
    server.shutdown().unwrap();
    cleanup(&directory);
}

struct TestIdentity {
    certificate: Certificate,
    private_key: KeyPair,
}

struct TestPki {
    ca_certificate: Certificate,
    server: TestIdentity,
    allowed_client: TestIdentity,
    unlisted_client: TestIdentity,
}

impl TestPki {
    fn generate() -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_certificate = ca_params.self_signed(&ca_key).unwrap();
        Self {
            server: generate_leaf(&ca_certificate, &ca_key, true),
            allowed_client: generate_leaf(&ca_certificate, &ca_key, false),
            unlisted_client: generate_leaf(&ca_certificate, &ca_key, false),
            ca_certificate,
        }
    }

    fn write(&self, directory: &Path) {
        std::fs::write(directory.join("ca.pem"), self.ca_certificate.pem()).unwrap();
        std::fs::write(directory.join("server.pem"), self.server.certificate.pem()).unwrap();
        std::fs::write(
            directory.join("server-key.pem"),
            self.server.private_key.serialize_pem(),
        )
        .unwrap();
        self.write_client(directory, "allowed", &self.allowed_client);
        self.write_client(directory, "unlisted", &self.unlisted_client);
    }

    fn write_client(&self, directory: &Path, name: &str, identity: &TestIdentity) {
        std::fs::write(
            directory.join(format!("{name}.pem")),
            identity.certificate.pem(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("{name}-key.pem")),
            identity.private_key.serialize_pem(),
        )
        .unwrap();
    }
}

fn generate_leaf(issuer: &Certificate, issuer_key: &KeyPair, server: bool) -> TestIdentity {
    let private_key = KeyPair::generate().unwrap();
    let names = if server {
        vec!["localhost".into(), Ipv4Addr::LOCALHOST.to_string()]
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
    let certificate = params.signed_by(&private_key, issuer, issuer_key).unwrap();
    TestIdentity {
        certificate,
        private_key,
    }
}

fn certificate_fingerprint(certificate: &Certificate) -> String {
    Sha256::digest(certificate.der())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn tls_manifest(pki: &TestPki) -> String {
    format!(
        r#"{{
            "version":4,
            "listen":"127.0.0.1:0",
            "tls":{{
                "server_certificate":"server.pem",
                "server_private_key":"server-key.pem",
                "client_ca":"ca.pem"
            }},
            "authorization":{{"clients":[{{
                "certificate_sha256":"{}",
                "tables":[{}]
            }}]}},
            "tables":[{{
                "path":"users.ndb","id":1,"name":"users",
                "columns":[
                    {{"id":1,"name":"id","physical_type":"int64","semantic_type":"UserId","nullable":false,"primary_key":true}},
                    {{"id":2,"name":"name","physical_type":"text","semantic_type":null,"nullable":true,"primary_key":false}}
                ]
            }}]
        }}"#,
        certificate_fingerprint(&pki.allowed_client.certificate),
        table_permissions()
    )
}

fn tls_client_config(directory: &Path, identity: &str) -> TlsConfig {
    TlsConfig::from_pem_files(
        "localhost",
        directory.join("ca.pem"),
        directory.join(format!("{identity}.pem")),
        directory.join(format!("{identity}-key.pem")),
    )
    .unwrap()
}

#[test]
fn mutual_tls_connects_allowlisted_client_and_rejects_unlisted_client() {
    let directory = test_directory("mtls");
    let table = create_database(&directory);
    let pki = TestPki::generate();
    pki.write(&directory);
    let server = start_server(&directory, tls_manifest(&pki));

    let valid_config = Config::new(server.local_addr().to_string())
        .tls(tls_client_config(&directory, "allowed"))
        .require_schema(TableIdentity::from_table(&table).unwrap())
        .read_timeout(Some(Duration::from_secs(5)))
        .write_timeout(Some(Duration::from_secs(5)));
    let mut client = Client::connect(valid_config).unwrap();
    client.ping().unwrap();
    assert!(read_ids(&mut client).is_empty());
    client.close().unwrap();
    wait_for_no_connections(&server);

    let unlisted_config = Config::new(server.local_addr().to_string())
        .tls(tls_client_config(&directory, "unlisted"))
        .read_timeout(Some(Duration::from_secs(5)))
        .write_timeout(Some(Duration::from_secs(5)));
    assert!(Client::connect(unlisted_config).is_err());
    wait_for_no_connections(&server);

    server.shutdown().unwrap();
    cleanup(&directory);
}

#[test]
fn tls_config_rejects_invalid_name_missing_certificates_and_multiple_keys() {
    let directory = test_directory("tls-config");
    cleanup(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let pki = TestPki::generate();
    pki.write(&directory);

    let valid = tls_client_config(&directory, "allowed");
    assert!(!format!("{valid:?}").contains("PRIVATE KEY"));
    assert!(matches!(
        TlsConfig::from_pem_files(
            "",
            directory.join("ca.pem"),
            directory.join("allowed.pem"),
            directory.join("allowed-key.pem"),
        ),
        Err(TlsConfigError::InvalidServerName(_))
    ));

    std::fs::write(directory.join("empty.pem"), "").unwrap();
    assert!(matches!(
        TlsConfig::from_pem_files(
            "localhost",
            directory.join("empty.pem"),
            directory.join("allowed.pem"),
            directory.join("allowed-key.pem"),
        ),
        Err(TlsConfigError::MissingCertificate {
            field: "root_ca",
            ..
        })
    ));

    std::fs::write(
        directory.join("two-keys.pem"),
        format!(
            "{}{}",
            pki.allowed_client.private_key.serialize_pem(),
            pki.unlisted_client.private_key.serialize_pem()
        ),
    )
    .unwrap();
    assert!(matches!(
        TlsConfig::from_pem_files(
            "localhost",
            directory.join("ca.pem"),
            directory.join("allowed.pem"),
            directory.join("two-keys.pem"),
        ),
        Err(TlsConfigError::MultiplePrivateKeys { count: 2, .. })
    ));
    cleanup(&directory);
}
