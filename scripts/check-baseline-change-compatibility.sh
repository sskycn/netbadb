#!/bin/sh
set -eu
repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compat_root=$(mktemp -d)
trap 'rm -rf "$compat_root"' EXIT HUP INT TERM
mkdir -p "$compat_root/baseline" "$compat_root/writer/src" "$compat_root/reader/src"
git -C "$repository" archive b36f784584132ce21fe24ed840da681613487881 | tar -xf - -C "$compat_root/baseline"
cat > "$compat_root/writer/Cargo.toml" <<EOF
[package]
name = "netbadb-compat-writer"
version = "0.0.0"
edition = "2024"

[dependencies]
netbadb-schema = { path = "$compat_root/baseline/crates/netbadb-schema" }
netbadb-storage = { path = "$compat_root/baseline/crates/netbadb-storage" }
netbadb-types = { path = "$compat_root/baseline/crates/netbadb-types" }
EOF
cat > "$compat_root/reader/Cargo.toml" <<EOF
[package]
name = "netbadb-compat-reader"
version = "0.0.0"
edition = "2024"

[dependencies]
netbadb-schema = { path = "$repository/crates/netbadb-schema" }
netbadb-storage = { path = "$repository/crates/netbadb-storage" }
netbadb-types = { path = "$repository/crates/netbadb-types" }
EOF
cat > "$compat_root/writer/src/main.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_DB").unwrap());
    let table = TableDef::new(TableId(7), "events", vec![
        ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64)),
    ]);
    let mut storage = TableStorage::create_heap_with_storage_id(path, table, StorageId(11)).unwrap();
    storage.enable_change_stream().unwrap();
    storage.insert(&[ScalarValue::Int64(17)]).unwrap();
    storage.close().unwrap();
}
RS
cp "$compat_root/writer/src/main.rs" "$compat_root/reader/src/main.rs"
cat > "$compat_root/reader/src/read.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ChangeStreamCursor, StorageChange, TableStorage};
use netbadb_types::{ChangeStreamGeneration, ColumnId, PhysicalType, ScalarValue, StorageDataVersion, StorageId, TableId};

fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_DB").unwrap());
    let table = TableDef::new(TableId(7), "events", vec![
        ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64)),
    ]);
    let initial = ChangeStreamCursor {
        storage_id: StorageId(11), generation: ChangeStreamGeneration(1), frontier: StorageDataVersion(0),
    };
    let mut storage = TableStorage::open_heap(&path, table.clone()).unwrap();
    let before = storage.read_changes(initial, 10, 1_000_000).unwrap();
    assert_eq!(before.batches.len(), 1);
    assert!(matches!(&before.batches[0].mutations[0], StorageChange::Insert { after, .. } if after == &[ScalarValue::Int64(17)]));
    storage.insert(&[ScalarValue::Int64(29)]).unwrap();
    storage.close().unwrap();
    let reopened = TableStorage::open_heap(&path, table).unwrap();
    let after = reopened.read_changes(initial, 10, 1_000_000).unwrap();
    assert_eq!(after.batches.len(), 2);
    assert!(matches!(&after.batches[1].mutations[0], StorageChange::Insert { after, .. } if after == &[ScalarValue::Int64(29)]));
    reopened.close().unwrap();
}
RS
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'

[[bin]]
name = "compat-read"
path = "src/read.rs"
EOF
NETBADB_COMPAT_DB="$compat_root/baseline.db" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml"
NETBADB_COMPAT_DB="$compat_root/current.db" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin netbadb-compat-reader
cmp "$compat_root/baseline.db.change" "$compat_root/current.db.change"
NETBADB_COMPAT_DB="$compat_root/baseline.db" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin compat-read

cat > "$compat_root/writer/src/lsm_write.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_LSM").unwrap());
    let table = TableDef::new(TableId(8), "lsm_events", vec![
        ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64)),
    ]);
    let mut storage = TableStorage::create_lsm(root, table, ColumnId(1)).unwrap();
    storage.insert(&[ScalarValue::Int64(17)]).unwrap();
    storage.close().unwrap();
}
RS
cat >> "$compat_root/writer/Cargo.toml" <<'EOF'

[[bin]]
name = "lsm-write"
path = "src/lsm_write.rs"
EOF
cp "$compat_root/writer/src/lsm_write.rs" "$compat_root/reader/src/lsm_write.rs"
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'

[[bin]]
name = "lsm-write"
path = "src/lsm_write.rs"
EOF
cat > "$compat_root/reader/src/lsm_read.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_LSM").unwrap());
    let table = TableDef::new(TableId(8), "lsm_events", vec![
        ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64)),
    ]);
    let mut storage = TableStorage::open_lsm(&root, table.clone()).unwrap();
    let view = storage.read_view().unwrap();
    assert_eq!(storage.scan_columns_with_view(&[ColumnId(1)], &view).unwrap()[0].1, vec![ScalarValue::Int64(17)]);
    drop(view);
    storage.insert(&[ScalarValue::Int64(29)]).unwrap();
    storage.close().unwrap();
    let mut reopened = TableStorage::open_lsm(&root, table).unwrap();
    let view = reopened.read_view().unwrap();
    let rows = reopened.scan_columns_with_view(&[ColumnId(1)], &view).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(17)]);
    assert_eq!(rows[1].1, vec![ScalarValue::Int64(29)]);
}
RS
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'

[[bin]]
name = "lsm-read"
path = "src/lsm_read.rs"
EOF
NETBADB_COMPAT_LSM="$compat_root/baseline-lsm" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin lsm-write
NETBADB_COMPAT_LSM="$compat_root/current-lsm" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin lsm-write
cmp "$compat_root/baseline-lsm/MANIFEST" "$compat_root/current-lsm/MANIFEST"
NETBADB_COMPAT_LSM="$compat_root/baseline-lsm" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin lsm-read
