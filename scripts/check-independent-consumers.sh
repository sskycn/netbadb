#!/bin/sh
set -eu
repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
echo "combined consumer: current=$(git -C "$repository" rev-parse HEAD) toolchain=$(rustc --version)"
consumer=$(mktemp -d)
trap 'rm -rf "$consumer"' EXIT HUP INT TERM
mkdir -p "$consumer/src"
cat > "$consumer/Cargo.toml" <<EOF
[package]
name = "netbadb-capability-consumer"
version = "0.0.0"
edition = "2024"

[dependencies]
netbadb-types = { path = "$repository/crates/netbadb-types" }
netbadb-schema = { path = "$repository/crates/netbadb-schema" }
netbadb-storage-api = { path = "$repository/crates/netbadb-storage-api" }
netbadb-row-codec = { path = "$repository/crates/netbadb-row-codec" }
netbadb-change-stream = { path = "$repository/crates/netbadb-change-stream" }
netbadb-lsm = { path = "$repository/crates/netbadb-lsm" }
netbadb-columnar = { path = "$repository/crates/netbadb-columnar" }
netbadb-heap = { path = "$repository/crates/netbadb-heap" }
netbadb-rel = { path = "$repository/crates/netbadb-rel" }
netbadb-planner = { path = "$repository/crates/netbadb-planner" }
netbadb-query-feedback = { path = "$repository/crates/netbadb-query-feedback" }
netbadb-advisor = { path = "$repository/crates/netbadb-advisor" }
EOF
cat > "$consumer/src/main.rs" <<'RS'
use netbadb_row_codec::{decode_row, encode_row};
use netbadb_lsm::LsmStorage;
use netbadb_columnar::{ColumnarProjection, StorageSnapshotToken};
use netbadb_heap::HeapStorage;
use netbadb_change_stream::{
    AuthoritativeOutcome, ChangeStorageKind, ChangeStreamManager, StorageChange, StorageVersionKey,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage_api::{IsolationLevel, StorageKind};
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, PageId, PhysicalType, RowId, ScalarValue, StorageDataVersion, StorageId, TableId, TxnId};

fn main() {
    let report = netbadb_query_feedback::correlate_execution_feedback(
        netbadb_query_feedback::ExecutionFeedbackAnchor {
            global_commit_seq: None,
            schema_generation: netbadb_types::SchemaGeneration(1),
        },
        netbadb_planner::PlannerCalibrationEpoch(1),
        netbadb_rel::LogicalQueryShape::OneRow,
        netbadb_planner::PlanVariant::OneRow,
        &[],
        netbadb_query_feedback::ExecutionStatistics::default(),
    );
    assert!(report.accesses.is_empty());
    let maintenance = netbadb_advisor::plan_maintenance(Vec::new(), None);
    assert!(maintenance.decision.is_none());
    let cadence = netbadb_advisor::AutomaticSchedulerPolicy::new(1, 2, 2, 3).expect("cadence");
    assert_eq!(
        netbadb_advisor::evaluate_scheduler_gate(
            netbadb_advisor::AutomaticSchedulerState::INITIAL,
            cadence,
            netbadb_advisor::AdaptiveEvidenceProgressToken::default(),
            netbadb_advisor::AutomaticSchedulerTick(1),
        ).expect("schedule"),
        netbadb_advisor::AutomaticSchedulerInspection::WouldRunNow,
    );
    let table = TableDef::new(
        TableId(1),
        "independent",
        vec![ColumnDef::new(ColumnId(1), "value", TypeSpec::Physical(PhysicalType::Int64))],
    );
    let expected = vec![ScalarValue::Int64(-7)];
    let encoded = encode_row(&expected).expect("encode");
    assert_eq!(encoded, vec![1, 249, 255, 255, 255, 255, 255, 255, 255]);
    assert_eq!(decode_row(&encoded, &table).expect("decode"), expected);
    assert_eq!(StorageKind::Heap, StorageKind::Heap);
    assert_eq!(IsolationLevel::ReadCommitted, IsolationLevel::ReadCommitted);

    let path = std::path::PathBuf::from(std::env::var_os("NETBADB_CONSUMER_LOG").unwrap());
    let mut stream = ChangeStreamManager::disabled(
        path.clone(), ChangeStorageKind::Heap, StorageId(5), &table,
    ).expect("create stream");
    let start = stream.enable(StorageDataVersion(0)).expect("enable");
    let change = StorageChange::Insert {
        new_version: StorageVersionKey::Heap {
            storage_id: StorageId(5),
            row_id: RowId { page: PageId(1), slot: 1, generation: 1 },
        },
        after: expected,
    };
    let prepared = stream.prepare(TxnId(1), None, &[change]).expect("prepare").unwrap();
    stream.publish(TxnId(1), prepared, None).expect("finalize after authoritative commit");
    assert_eq!(stream.read(start, 10, 1_048_576).expect("replay").batches.len(), 1);
    drop(stream);
    let reopened = ChangeStreamManager::open(
        path.clone(), ChangeStorageKind::Heap, StorageId(5), &table,
        |_| AuthoritativeOutcome::Committed(None),
    ).expect("reopen");
    assert_eq!(reopened.read(start, 10, 1_048_576).expect("replay after reopen").batches.len(), 1);

    let lsm_root = path.parent().unwrap().join("lsm");
    let mut lsm = LsmStorage::create(&lsm_root, table.clone(), ColumnId(1)).expect("create LSM");
    lsm.insert(&[ScalarValue::Int64(9)]).expect("insert LSM row");
    let view = lsm.read_view().expect("LSM read view");
    assert_eq!(lsm.scan_columns_with_view(&[ColumnId(1)], &view).expect("scan LSM").len(), 1);
    drop(view);
    lsm.close().expect("close LSM");
    let mut reopened_lsm = LsmStorage::open(&lsm_root, table).expect("reopen LSM");
    let view = reopened_lsm.read_view().expect("reopened LSM read view");
    assert_eq!(reopened_lsm.scan_columns_with_view(&[ColumnId(1)], &view).expect("replay LSM")[0].1, vec![ScalarValue::Int64(9)]);

    let columnar_root = path.parent().unwrap().join("columnar");
    ColumnarProjection::prepare(
        &columnar_root, ColumnarProjectionId(1), ColumnarGeneration(1),
        reopened_lsm.table(), StorageId(5), StorageSnapshotToken::heap(StorageId(5), 1),
        &[ColumnId(1)], &[vec![ScalarValue::Int64(11)]], None,
    ).expect("prepare Columnar artifact").publish().expect("publish Columnar artifact");
    let columnar = ColumnarProjection::open(&columnar_root, reopened_lsm.table()).expect("reopen Columnar artifact");
    let (batches, _) = columnar.scan(&[ColumnId(1)], &[]).expect("scan Columnar artifact");
    assert_eq!(batches.iter().map(|batch| batch.row_count).sum::<usize>(), 1);

    let heap_path = path.parent().unwrap().join("heap.db");
    let mut heap = HeapStorage::create(&heap_path, reopened_lsm.table().clone()).expect("create Heap");
    heap.create_index(ColumnId(1)).expect("create Heap index");
    heap.insert(&[ScalarValue::Int64(21)]).expect("commit Heap row");
    let mut rolled_back = heap.begin_transaction().expect("begin rollback");
    heap.insert_in(&mut rolled_back, &[ScalarValue::Int64(22)]).expect("insert rollback row");
    rolled_back.rollback().expect("rollback Heap row");
    assert_eq!(heap.scan().expect("scan Heap").len(), 1);
    heap.close().expect("close Heap");
    let mut reopened_heap = HeapStorage::open(&heap_path, reopened_lsm.table().clone()).expect("reopen Heap");
    assert_eq!(reopened_heap.scan().expect("replay Heap")[0].1, vec![ScalarValue::Int64(21)]);
}
RS
NETBADB_CONSUMER_LOG="$consumer/changes.nbcl" cargo run --quiet --offline --manifest-path "$consumer/Cargo.toml"
if cargo tree --offline --manifest-path "$consumer/Cargo.toml" -e normal,build | grep -E 'netbadb-(storage v|core v|executor v|server v)'; then
    echo 'independent consumer unexpectedly depends on database composition' >&2
    exit 1
fi
if cargo tree --offline --manifest-path "$consumer/Cargo.toml" -e features | grep 'test-hooks'; then
    echo 'normal independent consumer unexpectedly enables test hooks' >&2
    exit 1
fi
