use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, Schema, SchemaError, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde::Deserialize;

pub const DEPLOYMENT_MANIFEST_VERSION: u32 = 1;
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
        validate_loopback(listen)?;

        let manifest_directory = path
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| ManifestError::ManifestDirectory {
                path: path.to_path_buf(),
                source,
            })?;
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
        Ok(Self { listen, tables })
    }

    #[must_use]
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub fn tables(&self) -> &[TableBootstrap] {
        &self.tables
    }

    pub(crate) fn into_parts(self) -> (SocketAddr, Vec<TableBootstrap>) {
        (self.listen, self.tables)
    }
}

pub(crate) fn validate_loopback(address: SocketAddr) -> Result<(), ManifestError> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(ManifestError::RemoteListenRequiresNetworkHardening(address))
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
    RemoteListenRequiresNetworkHardening(SocketAddr),
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
            Self::RemoteListenRequiresNetworkHardening(address) => write!(
                formatter,
                "unauthenticated Phase 5B server only allows loopback addresses; `{address}` requires network hardening"
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
            Self::Schema(error) => error.fmt(formatter),
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. }
            | Self::ManifestDirectory { source, .. }
            | Self::TablePath { source, .. } => Some(source),
            Self::Json(error) => Some(error),
            Self::InvalidListen { source, .. } => Some(source),
            Self::Schema(error) => Some(error),
            Self::UnsupportedVersion(_)
            | Self::EmptyTables
            | Self::RemoteListenRequiresNetworkHardening(_)
            | Self::TablePathIsNotFile(_)
            | Self::DuplicateStoragePath(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentManifest {
    version: u32,
    listen: Option<String>,
    tables: Vec<ManifestTable>,
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
                "version": 1,
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

        std::fs::write(&manifest, r#"{"version":1,"unexpected":true,"tables":[]}"#).unwrap();
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
            Err(ManifestError::RemoteListenRequiresNetworkHardening(_))
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
}
