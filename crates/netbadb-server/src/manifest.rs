use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, Schema, SchemaError, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde::Deserialize;

use crate::tls::{MutualTlsConfig, TlsMaterialPaths, TransportSecurity};
use crate::{
    DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_RESULT_ROWS, DEFAULT_WRITE_TIMEOUT,
    ServerLimits, ServerLimitsError,
};
use crate::{TlsConfigError, TransportKind};

pub const DEPLOYMENT_MANIFEST_VERSION: u32 = 3;
pub const DEFAULT_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7878);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBootstrap {
    pub path: PathBuf,
    pub table: TableDef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    listen: SocketAddr,
    tables: Vec<TableBootstrap>,
    limits: ServerLimits,
    tls: Option<MutualTlsConfig>,
}

impl ServerConfig {
    pub fn from_manifest_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest: DeploymentManifest =
            serde_json::from_str(&source).map_err(ManifestError::Json)?;
        if manifest.version != DEPLOYMENT_MANIFEST_VERSION {
            return Err(ManifestError::UnsupportedVersion(manifest.version));
        }
        if manifest.tables.is_empty() {
            return Err(ManifestError::EmptyTables);
        }

        let listen = match manifest.listen {
            Some(value) => value
                .parse()
                .map_err(|source| ManifestError::InvalidListen { value, source })?,
            None => DEFAULT_LISTEN_ADDRESS,
        };
        let limits = manifest
            .limits
            .map_or_else(|| Ok(ServerLimits::default()), ManifestLimits::into_limits)
            .map_err(ManifestError::Limits)?;

        let manifest_directory = path
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| ManifestError::ManifestDirectory {
                path: path.to_path_buf(),
                source,
            })?;
        let tls = manifest
            .tls
            .map(|tls| {
                let paths = tls.resolve(&manifest_directory)?;
                MutualTlsConfig::load(paths).map_err(ManifestError::TlsConfiguration)
            })
            .transpose()?;
        validate_listener_security(listen, tls.is_some())?;
        let mut tables = Vec::with_capacity(manifest.tables.len());
        let mut paths = HashSet::with_capacity(manifest.tables.len());
        for table in manifest.tables {
            let configured_path = PathBuf::from(&table.path);
            let resolved_path = if configured_path.is_absolute() {
                configured_path
            } else {
                manifest_directory.join(configured_path)
            };
            let resolved_path =
                resolved_path
                    .canonicalize()
                    .map_err(|source| ManifestError::TablePath {
                        path: resolved_path,
                        source,
                    })?;
            let metadata =
                std::fs::metadata(&resolved_path).map_err(|source| ManifestError::TablePath {
                    path: resolved_path.clone(),
                    source,
                })?;
            if !metadata.is_file() {
                return Err(ManifestError::TablePathIsNotFile(resolved_path));
            }
            if !paths.insert(resolved_path.clone()) {
                return Err(ManifestError::DuplicateStoragePath(resolved_path));
            }

            let columns = table
                .columns
                .into_iter()
                .map(ManifestColumn::into_column)
                .collect();
            let table = TableDef::new(TableId(table.id), table.name, columns);
            table.validate().map_err(ManifestError::Schema)?;
            table.fingerprint().map_err(ManifestError::Schema)?;
            tables.push(TableBootstrap {
                path: resolved_path,
                table,
            });
        }
        Schema::new(tables.iter().map(|entry| entry.table.clone()).collect())
            .map_err(ManifestError::Schema)?;
        Ok(Self {
            listen,
            tables,
            limits,
            tls,
        })
    }

    #[must_use]
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub fn tables(&self) -> &[TableBootstrap] {
        &self.tables
    }

    #[must_use]
    pub const fn limits(&self) -> ServerLimits {
        self.limits
    }

    #[must_use]
    pub const fn transport_kind(&self) -> TransportKind {
        if self.tls.is_some() {
            TransportKind::MutualTls
        } else {
            TransportKind::PlaintextLoopback
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SocketAddr,
        Vec<TableBootstrap>,
        ServerLimits,
        TransportSecurity,
    ) {
        let security = self
            .tls
            .map_or(TransportSecurity::PlaintextLoopback, |tls| {
                tls.into_transport()
            });
        (self.listen, self.tables, self.limits, security)
    }
}

pub(crate) fn validate_listener_security(
    address: SocketAddr,
    mutual_tls: bool,
) -> Result<(), ManifestError> {
    if address.ip().is_loopback() || mutual_tls {
        Ok(())
    } else {
        Err(ManifestError::RemoteListenRequiresMutualTls(address))
    }
}

#[derive(Debug)]
pub enum ManifestError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    EmptyTables,
    InvalidListen {
        value: String,
        source: AddrParseError,
    },
    RemoteListenRequiresMutualTls(SocketAddr),
    ManifestDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    TablePath {
        path: PathBuf,
        source: std::io::Error,
    },
    TablePathIsNotFile(PathBuf),
    DuplicateStoragePath(PathBuf),
    Limits(ServerLimitsError),
    TlsPath {
        field: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    TlsPathIsNotFile {
        field: &'static str,
        path: PathBuf,
    },
    TlsConfiguration(TlsConfigError),
    Schema(SchemaError),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read manifest `{}`: {source}",
                    path.display()
                )
            }
            Self::Json(error) => write!(formatter, "invalid deployment manifest JSON: {error}"),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported deployment manifest version {version}; expected {DEPLOYMENT_MANIFEST_VERSION}"
            ),
            Self::EmptyTables => {
                formatter.write_str("deployment manifest requires at least one table")
            }
            Self::InvalidListen { value, .. } => {
                write!(formatter, "invalid TCP listen address `{value}`")
            }
            Self::RemoteListenRequiresMutualTls(address) => write!(
                formatter,
                "non-loopback listener `{address}` requires mutual TLS"
            ),
            Self::ManifestDirectory { path, source } => write!(
                formatter,
                "failed to resolve directory containing manifest `{}`: {source}",
                path.display()
            ),
            Self::TablePath { path, source } => write!(
                formatter,
                "failed to resolve table file `{}`: {source}",
                path.display()
            ),
            Self::TablePathIsNotFile(path) => {
                write!(formatter, "table path `{}` is not a file", path.display())
            }
            Self::DuplicateStoragePath(path) => write!(
                formatter,
                "table file `{}` is configured more than once",
                path.display()
            ),
            Self::Limits(error) => error.fmt(formatter),
            Self::TlsPath {
                field,
                path,
                source,
            } => write!(
                formatter,
                "failed to resolve TLS field `{field}` path `{}`: {source}",
                path.display()
            ),
            Self::TlsPathIsNotFile { field, path } => write!(
                formatter,
                "TLS field `{field}` path `{}` is not a file",
                path.display()
            ),
            Self::TlsConfiguration(error) => error.fmt(formatter),
            Self::Schema(error) => error.fmt(formatter),
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. }
            | Self::ManifestDirectory { source, .. }
            | Self::TablePath { source, .. }
            | Self::TlsPath { source, .. } => Some(source),
            Self::Json(error) => Some(error),
            Self::InvalidListen { source, .. } => Some(source),
            Self::Limits(error) => Some(error),
            Self::TlsConfiguration(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::UnsupportedVersion(_)
            | Self::EmptyTables
            | Self::RemoteListenRequiresMutualTls(_)
            | Self::TablePathIsNotFile(_)
            | Self::DuplicateStoragePath(_)
            | Self::TlsPathIsNotFile { .. } => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentManifest {
    version: u32,
    listen: Option<String>,
    limits: Option<ManifestLimits>,
    tls: Option<ManifestTls>,
    tables: Vec<ManifestTable>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTls {
    server_certificate: String,
    server_private_key: String,
    client_ca: String,
}

impl ManifestTls {
    fn resolve(self, manifest_directory: &Path) -> Result<TlsMaterialPaths, ManifestError> {
        Ok(TlsMaterialPaths {
            server_certificate: resolve_tls_path(
                manifest_directory,
                "server_certificate",
                self.server_certificate,
            )?,
            server_private_key: resolve_tls_path(
                manifest_directory,
                "server_private_key",
                self.server_private_key,
            )?,
            client_ca: resolve_tls_path(manifest_directory, "client_ca", self.client_ca)?,
        })
    }
}

fn resolve_tls_path(
    manifest_directory: &Path,
    field: &'static str,
    configured: String,
) -> Result<PathBuf, ManifestError> {
    let configured = PathBuf::from(configured);
    let resolved = if configured.is_absolute() {
        configured
    } else {
        manifest_directory.join(configured)
    };
    let resolved = resolved
        .canonicalize()
        .map_err(|source| ManifestError::TlsPath {
            field,
            path: resolved,
            source,
        })?;
    let metadata = std::fs::metadata(&resolved).map_err(|source| ManifestError::TlsPath {
        field,
        path: resolved.clone(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(ManifestError::TlsPathIsNotFile {
            field,
            path: resolved,
        });
    }
    Ok(resolved)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLimits {
    max_connections: Option<u64>,
    idle_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    max_result_rows: Option<u64>,
}

impl ManifestLimits {
    fn into_limits(self) -> Result<ServerLimits, ServerLimitsError> {
        ServerLimits::from_millis(
            self.max_connections
                .unwrap_or(DEFAULT_MAX_CONNECTIONS as u64),
            self.idle_timeout_ms
                .unwrap_or(DEFAULT_IDLE_TIMEOUT.as_millis() as u64),
            self.write_timeout_ms
                .unwrap_or(DEFAULT_WRITE_TIMEOUT.as_millis() as u64),
            self.max_result_rows
                .unwrap_or(DEFAULT_MAX_RESULT_ROWS as u64),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTable {
    path: String,
    id: u64,
    name: String,
    columns: Vec<ManifestColumn>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestColumn {
    id: u32,
    name: String,
    physical_type: ManifestPhysicalType,
    semantic_type: Option<String>,
    nullable: bool,
    primary_key: bool,
}

impl ManifestColumn {
    fn into_column(self) -> ColumnDef {
        let physical = self.physical_type.into_physical();
        let type_spec = match self.semantic_type {
            Some(name) => TypeSpec::Semantic { name, physical },
            None => TypeSpec::Physical(physical),
        };
        ColumnDef::new(ColumnId(self.id), self.name, type_spec)
            .nullable(self.nullable)
            .primary_key(self.primary_key)
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ManifestPhysicalType {
    Bool,
    Int64,
    Uint64,
    Text,
}

impl ManifestPhysicalType {
    const fn into_physical(self) -> PhysicalType {
        match self {
            Self::Bool => PhysicalType::Bool,
            Self::Int64 => PhysicalType::Int64,
            Self::Uint64 => PhysicalType::UInt64,
            Self::Text => PhysicalType::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use netbadb_core::Database;
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("netbadb-manifest-{name}-{}", std::process::id()))
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
                        physical: PhysicalType::UInt64,
                    },
                )
                .primary_key(true),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )
    }

    fn create_heap(path: &Path) {
        Database::create(path, users_table("UserId"))
            .unwrap()
            .close()
            .unwrap();
    }

    fn manifest_json(listen: Option<&str>, path: &str, semantic_name: &str) -> String {
        let listen = listen.map_or_else(String::new, |listen| format!("\"listen\": \"{listen}\","));
        format!(
            r#"{{
                "version": 3,
                {listen}
                "tables": [{{
                    "path": "{path}",
                    "id": 1,
                    "name": "users",
                    "columns": [
                        {{
                            "id": 1,
                            "name": "id",
                            "physical_type": "uint64",
                            "semantic_type": "{semantic_name}",
                            "nullable": false,
                            "primary_key": true
                        }},
                        {{
                            "id": 2,
                            "name": "name",
                            "physical_type": "text",
                            "semantic_type": null,
                            "nullable": false,
                            "primary_key": false
                        }}
                    ]
                }}]
            }}"#
        )
    }

    #[test]
    fn relative_paths_and_full_table_defs_are_resolved_from_the_manifest() {
        let directory = test_directory("relative");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(directory.join("data")).unwrap();
        let heap = directory.join("data/users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, manifest_json(None, "data/users.ndb", "UserId")).unwrap();

        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.listen(), DEFAULT_LISTEN_ADDRESS);
        assert_eq!(config.limits(), ServerLimits::default());
        assert_eq!(config.tables().len(), 1);
        assert_eq!(config.tables()[0].path, heap.canonicalize().unwrap());
        assert_eq!(config.tables()[0].table, users_table("UserId"));
        assert_eq!(
            config.tables()[0].table.fingerprint().unwrap(),
            users_table("UserId").fingerprint().unwrap()
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unknown_versions_fields_remote_listeners_and_missing_paths() {
        let directory = test_directory("invalid");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let manifest = directory.join("server.json");

        std::fs::write(&manifest, r#"{"version":2,"tables":[]}"#).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::UnsupportedVersion(2))
        ));

        std::fs::write(&manifest, r#"{"version":3,"unexpected":true,"tables":[]}"#).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::write(
            &manifest,
            manifest_json(Some("0.0.0.0:7878"), "missing.ndb", "UserId"),
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::RemoteListenRequiresMutualTls(_))
        ));

        std::fs::write(
            &manifest,
            manifest_json(Some("127.0.0.1:0"), "missing.ndb", "UserId"),
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::TablePath { .. })
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn limits_are_partial_strict_and_bounded() {
        let directory = test_directory("limits");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let heap = directory.join("users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");

        let source = manifest_json(None, "users.ndb", "UserId").replace(
            "\"tables\":",
            "\"limits\": {\"max_connections\": 2, \"idle_timeout_ms\": 250, \"max_result_rows\": 3}, \"tables\":",
        );
        std::fs::write(&manifest, source).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.limits().max_connections(), 2);
        assert_eq!(config.limits().idle_timeout(), Duration::from_millis(250));
        assert_eq!(config.limits().write_timeout(), DEFAULT_WRITE_TIMEOUT);
        assert_eq!(config.limits().max_result_rows(), 3);

        let invalid = manifest_json(None, "users.ndb", "UserId").replace(
            "\"tables\":",
            "\"limits\": {\"max_connections\": 0}, \"tables\":",
        );
        std::fs::write(&manifest, invalid).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Limits(error)) if error.field() == "max_connections"
        ));

        let unknown = manifest_json(None, "users.ndb", "UserId").replace(
            "\"tables\":",
            "\"limits\": {\"connection_limit\": 2}, \"tables\":",
        );
        std::fs::write(&manifest, unknown).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }
}
