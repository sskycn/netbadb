#!/bin/sh
set -eu
repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
baseline=b36f784584132ce21fe24ed840da681613487881
echo "compatibility: current=$(git -C "$repository" rev-parse HEAD) baseline=$baseline toolchain=$(rustc --version)"
compat_root=$(mktemp -d)
trap 'rm -rf "$compat_root"' EXIT HUP INT TERM
mkdir -p "$compat_root/baseline" "$compat_root/writer/src" "$compat_root/reader/src"
git -C "$repository" archive "$baseline" | tar -xf - -C "$compat_root/baseline"
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
echo "compatibility: resolving external baseline and current Cargo projects offline"
cargo metadata --offline --format-version 1 --manifest-path "$compat_root/writer/Cargo.toml" > /dev/null
cargo metadata --offline --format-version 1 --manifest-path "$compat_root/reader/Cargo.toml" > /dev/null
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
    let view = storage.read_view().unwrap();
    assert_eq!(storage.scan_columns_with_view(&[ColumnId(1)], &view).unwrap()[0].1, vec![ScalarValue::Int64(17)]);
    drop(view);
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

# Read-only binaries compile against each version independently. They verify the
# reverse direction and the baseline's view after the current version appended.
cat > "$compat_root/reader/src/heap_verify.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ChangeStreamCursor, StorageChange, TableStorage};
use netbadb_types::{ChangeStreamGeneration, ColumnId, PhysicalType, ScalarValue, StorageDataVersion, StorageId, TableId};
fn main() {
    let path = std::env::var_os("NETBADB_COMPAT_DB").unwrap();
    let expected: usize = std::env::var("NETBADB_EXPECT_ROWS").unwrap().parse().unwrap();
    let table = TableDef::new(TableId(7), "events", vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut storage = TableStorage::open_heap(&path, table).unwrap();
    let cursor = ChangeStreamCursor { storage_id: StorageId(11), generation: ChangeStreamGeneration(1), frontier: StorageDataVersion(0) };
    let changes = storage.read_changes(cursor, 10, 1_000_000).unwrap();
    assert_eq!(changes.batches.len(), expected);
    assert!(matches!(&changes.batches[0].mutations[0], StorageChange::Insert { after, .. } if after == &[ScalarValue::Int64(17)]));
    let view = storage.read_view().unwrap();
    let rows = storage.scan_columns_with_view(&[ColumnId(1)], &view).unwrap();
    assert_eq!(rows.len(), expected);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(17)]);
    drop(view);
    storage.close().unwrap();
}
RS
cp "$compat_root/reader/src/heap_verify.rs" "$compat_root/writer/src/heap_verify.rs"
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'
[[bin]]
name = "heap-verify"
path = "src/heap_verify.rs"
EOF
cat >> "$compat_root/writer/Cargo.toml" <<'EOF'
[[bin]]
name = "heap-verify"
path = "src/heap_verify.rs"
EOF
NETBADB_COMPAT_DB="$compat_root/baseline.db" NETBADB_EXPECT_ROWS=2 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin heap-verify
NETBADB_COMPAT_DB="$compat_root/current.db" NETBADB_EXPECT_ROWS=1 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin heap-verify
NETBADB_COMPAT_DB="$compat_root/baseline.db" NETBADB_EXPECT_ROWS=2 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin heap-verify

cat > "$compat_root/reader/src/lsm_verify.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
fn main() {
    let path = std::env::var_os("NETBADB_COMPAT_LSM").unwrap();
    let expected: usize = std::env::var("NETBADB_EXPECT_ROWS").unwrap().parse().unwrap();
    let table = TableDef::new(TableId(8), "lsm_events", vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut storage = TableStorage::open_lsm(&path, table).unwrap();
    let view = storage.read_view().unwrap();
    let rows = storage.scan_columns_with_view(&[ColumnId(1)], &view).unwrap();
    assert_eq!(rows.len(), expected);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(17)]);
    drop(view);
    storage.close().unwrap();
}
RS
cp "$compat_root/reader/src/lsm_verify.rs" "$compat_root/writer/src/lsm_verify.rs"
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'
[[bin]]
name = "lsm-verify"
path = "src/lsm_verify.rs"
EOF
cat >> "$compat_root/writer/Cargo.toml" <<'EOF'
[[bin]]
name = "lsm-verify"
path = "src/lsm_verify.rs"
EOF
NETBADB_COMPAT_LSM="$compat_root/baseline-lsm" NETBADB_EXPECT_ROWS=2 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin lsm-verify
NETBADB_COMPAT_LSM="$compat_root/current-lsm" NETBADB_EXPECT_ROWS=1 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin lsm-verify
NETBADB_COMPAT_LSM="$compat_root/baseline-lsm" NETBADB_EXPECT_ROWS=2 CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin lsm-verify

# A Columnar artifact is derived data. Exercise both read directions for a
# published base artifact; the Core catalog owns managed generation changes.
cat > "$compat_root/reader/src/columnar_write.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ColumnarProjection, TableStorage};
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, PhysicalType, ScalarValue, StorageId, TableId};
fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_COLUMNAR").unwrap());
    let table = TableDef::new(TableId(9), "columnar_events", vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut source = TableStorage::create_heap_with_storage_id(root.with_extension("db"), table.clone(), StorageId(13)).unwrap();
    source.insert(&[ScalarValue::Int64(17)]).unwrap();
    let token = source.current_snapshot_token().unwrap();
    ColumnarProjection::prepare(&root, ColumnarProjectionId(1), ColumnarGeneration(1), &table,
        StorageId(13), token, &[ColumnId(1)], &[vec![ScalarValue::Int64(17)]], None).unwrap().publish().unwrap();
    source.close().unwrap();
}
RS
cp "$compat_root/reader/src/columnar_write.rs" "$compat_root/writer/src/columnar_write.rs"
cat > "$compat_root/reader/src/columnar_verify.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::ColumnarProjection;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
fn main() {
    let root = std::env::var_os("NETBADB_COMPAT_COLUMNAR").unwrap();
    let table = TableDef::new(TableId(9), "columnar_events", vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))]);
    let artifact = ColumnarProjection::open(&root, &table).unwrap();
    let (batches, _) = artifact.scan(&[ColumnId(1)], &[]).unwrap();
    assert_eq!(batches.iter().map(|batch| batch.row_count).sum::<usize>(), 1);
    assert_eq!(batches[0].columns[0].values.value(0).unwrap(), ScalarValue::Int64(17));
}
RS
cp "$compat_root/reader/src/columnar_verify.rs" "$compat_root/writer/src/columnar_verify.rs"
for project in writer reader; do
    cat >> "$compat_root/$project/Cargo.toml" <<'EOF'
[[bin]]
name = "columnar-write"
path = "src/columnar_write.rs"
[[bin]]
name = "columnar-verify"
path = "src/columnar_verify.rs"
EOF
done
NETBADB_COMPAT_COLUMNAR="$compat_root/baseline-columnar" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin columnar-write
NETBADB_COMPAT_COLUMNAR="$compat_root/current-columnar" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin columnar-write
cmp "$compat_root/baseline-columnar/projection.nbcmanifest" "$compat_root/current-columnar/projection.nbcmanifest"
baseline_segment=$(find "$compat_root/baseline-columnar" -name '*.nbcs' -print)
current_segment=$(find "$compat_root/current-columnar" -name '*.nbcs' -print)
test -n "$baseline_segment" && test -n "$current_segment"
cmp "$baseline_segment" "$current_segment"
NETBADB_COMPAT_COLUMNAR="$compat_root/baseline-columnar" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin columnar-verify
NETBADB_COMPAT_COLUMNAR="$compat_root/current-columnar" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin columnar-verify
NETBADB_COMPAT_COLUMNAR="$compat_root/current-columnar" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin columnar-verify

# Compile representative old-path direct calls against both versions. The
# concrete error return types changed, so use inference where old call sites
# only handled errors through the facade's structured StorageError.
cat > "$compat_root/reader/src/direct_api.rs" <<'RS'
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, LsmStorage, StorageError, StorageKind, StorageVisibilityBoundary};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
fn table() -> TableDef {
    TableDef::new(TableId(10), "direct", vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))])
}
fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("NETBADB_COMPAT_API").unwrap());
    let mut heap = HeapStorage::create(root.with_extension("db"), table()).unwrap();
    heap.insert(&[ScalarValue::Int64(1)]).unwrap();
    assert_eq!(heap.scan().unwrap().len(), 1);
    heap.close().unwrap();
    let mut heap = HeapStorage::open(root.with_extension("db"), table()).unwrap();
    assert_eq!(heap.scan().unwrap().len(), 1);
    heap.close().unwrap();
    let lsm = LsmStorage::create(root.with_extension("lsm"), table(), ColumnId(1)).unwrap();
    lsm.close().unwrap();
    LsmStorage::open(root.with_extension("lsm"), table()).unwrap().close().unwrap();
    let boundary = StorageVisibilityBoundary::new(StorageId(1), StorageKind::Heap, 1).unwrap();
    assert_eq!(boundary.storage_id(), StorageId(1));
    let _: Option<StorageError> = None;
}
RS
cp "$compat_root/reader/src/direct_api.rs" "$compat_root/writer/src/direct_api.rs"
for project in writer reader; do
    cat >> "$compat_root/$project/Cargo.toml" <<'EOF'
[[bin]]
name = "direct-api"
path = "src/direct_api.rs"
EOF
done
NETBADB_COMPAT_API="$compat_root/baseline-direct" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/writer/Cargo.toml" --bin direct-api
NETBADB_COMPAT_API="$compat_root/current-direct" CARGO_TARGET_DIR="$repository/target" cargo run --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin direct-api
cat >> "$compat_root/reader/Cargo.toml" <<EOF
[dependencies.netbadb-heap]
path = "$repository/crates/netbadb-heap"
[dependencies.netbadb-lsm]
path = "$repository/crates/netbadb-lsm"
[dependencies.netbadb-storage-api]
path = "$repository/crates/netbadb-storage-api"
EOF
cat > "$compat_root/reader/src/current_api.rs" <<'RS'
use netbadb_heap::{HeapStorage, HeapStorageError, RecoveryError};
use netbadb_lsm::{LsmStorage, LsmStorageError};
use netbadb_storage::{StorageError, StorageVisibilityBoundary};
use netbadb_storage_api::{StorageKind, VisibilityBoundaryError};
use netbadb_schema::TableDef;
use netbadb_types::{ColumnId, StorageId};
fn heap_result(_: Result<HeapStorage, HeapStorageError>) {}
fn lsm_result(_: Result<LsmStorage, LsmStorageError>) {}
fn heap_to_facade(error: HeapStorageError) -> StorageError { error.into() }
fn lsm_to_facade(error: LsmStorageError) -> StorageError { error.into() }
fn recovery_source(error: RecoveryError) {
    if let RecoveryError::Storage(source) = error {
        let _: Box<HeapStorageError> = source;
    }
}
fn typed_operations(table: TableDef) {
    heap_result(HeapStorage::create("/unused/heap", table.clone()));
    lsm_result(LsmStorage::create("/unused/lsm", table, ColumnId(1)));
}
fn main() {
    let _: Option<netbadb_heap::HeapStorage> = None::<netbadb_storage::HeapStorage>;
    let _: Option<netbadb_lsm::LsmStorage> = None::<netbadb_storage::LsmStorage>;
    let _: Result<StorageVisibilityBoundary, VisibilityBoundaryError> =
        StorageVisibilityBoundary::new(StorageId(1), StorageKind::Heap, 1);
    let _ = (typed_operations, heap_to_facade, lsm_to_facade, recovery_source);
}
RS
cat >> "$compat_root/reader/Cargo.toml" <<'EOF'
[[bin]]
name = "current-api"
path = "src/current_api.rs"
EOF
CARGO_TARGET_DIR="$repository/target" cargo check --quiet --offline --manifest-path "$compat_root/reader/Cargo.toml" --bin current-api
echo "compatibility: Heap/NBCL, LSM, Columnar and old-path direct API cases passed"
