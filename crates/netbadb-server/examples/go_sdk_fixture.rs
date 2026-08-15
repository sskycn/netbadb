use std::error::Error;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use netbadb_core::Database;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_server::{ServerConfig, TcpServer};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use serde_json::json;
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn Error>> {
    let transport = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "plaintext".into());
    if transport != "plaintext" && transport != "mtls" {
        return Err("transport must be `plaintext` or `mtls`".into());
    }
    let directory = fixture_directory()?;
    std::fs::create_dir(&directory)?;
    let result = run(&directory, &transport);
    let _ = std::fs::remove_dir_all(&directory);
    result
}

fn run(directory: &Path, transport: &str) -> Result<(), Box<dyn Error>> {
    let table = users_table();
    let heap = directory.join("users.ndb");
    Database::create(&heap, table.clone())?.close()?;
    let fingerprint = table.fingerprint()?;

    let (tls, authorization, client) = if transport == "mtls" {
        let pki = TestPki::generate()?;
        let material = pki.write(directory)?;
        let listed_fingerprint = hex(&Sha256::digest(pki.client_certificate.der()));
        (
            Some(json!({
                "server_certificate": "server.pem",
                "server_private_key": "server-key.pem",
                "client_ca": "client-ca.pem"
            })),
            json!({"clients": [{
                "certificate_sha256": listed_fingerprint,
                "tables": [permissions()]
            }]}),
            Some(material),
        )
    } else {
        (
            None,
            json!({"local_plaintext": {"tables": [permissions()]}, "clients": []}),
            None,
        )
    };

    let mut manifest = json!({
        "version": 4,
        "listen": "127.0.0.1:0",
        "authorization": authorization,
        "tables": [{
            "path": "users.ndb",
            "id": 1,
            "name": "users",
            "columns": [
                {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": "UserId", "nullable": false, "primary_key": true},
                {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
            ]
        }]
    });
    if let Some(tls) = tls {
        manifest["tls"] = tls;
    }
    let manifest_path = directory.join("server.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    let server = TcpServer::new(ServerConfig::from_manifest_path(&manifest_path)?).start()?;

    let mut ready = json!({
        "address": server.local_addr().to_string(),
        "transport": transport,
        "table_id": 1,
        "fingerprint": hex(fingerprint.as_bytes()),
    });
    if let Some(client) = client {
        ready["client_ca"] = json!(client.client_ca);
        ready["client_certificate"] = json!(client.client_certificate);
        ready["client_private_key"] = json!(client.client_private_key);
        ready["unlisted_client_certificate"] = json!(client.unlisted_client_certificate);
        ready["unlisted_client_private_key"] = json!(client.unlisted_client_private_key);
    }
    println!("{}", serde_json::to_string(&ready)?);
    io::stdout().flush()?;

    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    server.shutdown()?;
    Ok(())
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

fn permissions() -> serde_json::Value {
    json!({"table_id": 1, "read": true, "write": true, "transaction": true, "analyze": true})
}

fn fixture_directory() -> Result<PathBuf, Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "netbadb-go-sdk-fixture-{}-{nonce}",
        std::process::id()
    )))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct ClientMaterial {
    client_ca: String,
    client_certificate: String,
    client_private_key: String,
    unlisted_client_certificate: String,
    unlisted_client_private_key: String,
}

struct TestPki {
    ca_certificate: Certificate,
    server_certificate: Certificate,
    server_private_key: KeyPair,
    client_certificate: Certificate,
    client_private_key: KeyPair,
    unlisted_client_certificate: Certificate,
    unlisted_client_private_key: KeyPair,
}

impl TestPki {
    fn generate() -> Result<Self, Box<dyn Error>> {
        let ca_key = KeyPair::generate()?;
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_certificate = ca_params.self_signed(&ca_key)?;
        let (server_certificate, server_private_key) =
            generate_leaf(&ca_certificate, &ca_key, true)?;
        let (client, client_private_key) = generate_leaf(&ca_certificate, &ca_key, false)?;
        let (unlisted, unlisted_client_private_key) =
            generate_leaf(&ca_certificate, &ca_key, false)?;
        Ok(Self {
            ca_certificate,
            server_certificate,
            server_private_key,
            client_certificate: client,
            client_private_key,
            unlisted_client_certificate: unlisted,
            unlisted_client_private_key,
        })
    }

    fn write(&self, directory: &Path) -> Result<ClientMaterial, Box<dyn Error>> {
        let client_ca = directory.join("client-ca.pem");
        let client_certificate = directory.join("client.pem");
        let client_private_key = directory.join("client-key.pem");
        let unlisted_client_certificate = directory.join("unlisted-client.pem");
        let unlisted_client_private_key = directory.join("unlisted-client-key.pem");
        std::fs::write(directory.join("server.pem"), self.server_certificate.pem())?;
        std::fs::write(
            directory.join("server-key.pem"),
            self.server_private_key.serialize_pem(),
        )?;
        std::fs::write(&client_ca, self.ca_certificate.pem())?;
        std::fs::write(&client_certificate, self.client_certificate.pem())?;
        std::fs::write(&client_private_key, self.client_private_key.serialize_pem())?;
        std::fs::write(
            &unlisted_client_certificate,
            self.unlisted_client_certificate.pem(),
        )?;
        std::fs::write(
            &unlisted_client_private_key,
            self.unlisted_client_private_key.serialize_pem(),
        )?;
        Ok(ClientMaterial {
            client_ca: path_string(&client_ca),
            client_certificate: path_string(&client_certificate),
            client_private_key: path_string(&client_private_key),
            unlisted_client_certificate: path_string(&unlisted_client_certificate),
            unlisted_client_private_key: path_string(&unlisted_client_private_key),
        })
    }
}

fn generate_leaf(
    issuer: &Certificate,
    issuer_key: &KeyPair,
    server: bool,
) -> Result<(Certificate, KeyPair), Box<dyn Error>> {
    let key = KeyPair::generate()?;
    let names = if server {
        vec!["localhost".into(), "127.0.0.1".into()]
    } else {
        Vec::new()
    };
    let mut params = CertificateParams::new(names)?;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![if server {
        ExtendedKeyUsagePurpose::ServerAuth
    } else {
        ExtendedKeyUsagePurpose::ClientAuth
    }];
    Ok((params.signed_by(&key, issuer, issuer_key)?, key))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
