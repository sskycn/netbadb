use crate::authorization::{
    AuthorizationPolicy, PrincipalAuthorization, PrincipalGrants, TablePermissions,
};
use crate::{ClientIdentity, TransportKind};
use netbadb_core::{Database, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use std::path::{Path, PathBuf};

pub(crate) const CREATE: &str =
    "CREATE TABLE projects (id BIGINT NOT NULL, name TEXT, active BOOLEAN NOT NULL)";
pub(crate) fn seed(name: &str) -> (PathBuf, Database) {
    let root = std::env::temp_dir().join(format!(
        "netbadb-round19-server-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("users"),
            TableDef::new(
                TableId(1),
                "users",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        None,
    )
    .unwrap();
    (root, db)
}

pub(crate) fn managed_seed(name: &str) -> (PathBuf, Database) {
    let root = std::env::temp_dir().join(format!(
        "netbadb-round30-server-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut db = Database::create_catalog(root.join("catalog"), Vec::new(), None).unwrap();
    db.execute("CREATE TABLE users (id BIGINT)").unwrap();
    (root, db)
}
pub(crate) fn principal(schema_admin: bool) -> PrincipalAuthorization {
    AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin,
            tables: vec![TablePermissions::new(TableId(1), true, true, true, false)],
        }),
        vec![],
        &[TableId(1)],
    )
    .unwrap()
    .admit(&ClientIdentity::LocalPlaintext)
    .unwrap()
}
pub(crate) fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    fn collect(root: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(&path, files);
            } else {
                files.push((path.clone(), std::fs::read(path).unwrap()));
            }
        }
    }
    collect(root, &mut files);
    files.sort();
    files
}
