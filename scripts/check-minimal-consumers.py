#!/usr/bin/env python3
"""Build and run seven separate, workspace-external capability consumers."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

REPO = Path(__file__).resolve().parent.parent
BASE = {
    "row-codec": ["schema", "types"],
    "change-stream": ["schema", "types"],
    "lsm": ["schema", "types"],
    "columnar": ["schema", "types"],
    "heap": ["schema", "types"],
    "query-feedback": ["planner", "rel", "types"],
    "advisor": ["types", "storage-api", "schema", "lsm", "columnar"],
}
SOURCES = {
"row-codec": r'''
use netbadb_row_codec::{decode_row, encode_row, validate_row, RowError};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
fn main() {
    let table = TableDef::new(TableId(1), "rows", vec![
        ColumnDef::new(ColumnId(1), "number", TypeSpec::Physical(PhysicalType::Int64)),
        ColumnDef::new(ColumnId(2), "nullable", TypeSpec::Physical(PhysicalType::Int64)).nullable(true),
    ]);
    let values = vec![ScalarValue::Int64(-7), ScalarValue::Null];
    validate_row(&table, &values).unwrap();
    assert_eq!(decode_row(&encode_row(&values).unwrap(), &table).unwrap(), values);
    assert!(matches!(validate_row(&table, &[ScalarValue::Null, ScalarValue::Null]), Err(RowError::NullNotAllowed { .. })));
    assert!(matches!(validate_row(&table, &[ScalarValue::Bool(true), ScalarValue::Null]), Err(RowError::TypeMismatch { .. })));
    assert!(decode_row(&[255], &table).is_err());
}
''',
"change-stream": r'''
use netbadb_change_stream::{AuthoritativeOutcome, ChangeStorageKind, ChangeStreamManager, StorageChange, StorageVersionKey};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PageId, PhysicalType, RowId, ScalarValue, StorageDataVersion, StorageId, TableId, TxnId};
fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("CASE_PATH").unwrap());
    let table = TableDef::new(TableId(1), "changes", vec![ColumnDef::new(ColumnId(1), "n", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut stream = ChangeStreamManager::disabled(path.clone(), ChangeStorageKind::Heap, StorageId(5), &table).unwrap();
    let start = stream.enable(StorageDataVersion(0)).unwrap();
    let change = StorageChange::Insert { new_version: StorageVersionKey::Heap {
        storage_id: StorageId(5), row_id: RowId { page: PageId(1), slot: 1, generation: 1 },
    }, after: vec![ScalarValue::Int64(8)] };
    let prepared = stream.prepare(TxnId(1), None, &[change]).unwrap().unwrap();
    // The test harness owns a durable decision ledger for this standalone stream.
    let decision = path.with_extension("decision");
    std::fs::write(&decision, 1_u64.to_le_bytes()).unwrap();
    std::fs::File::open(&decision).unwrap().sync_all().unwrap();
    assert_eq!(std::fs::read(&decision).unwrap(), TxnId(1).0.to_le_bytes());
    stream.publish(TxnId(1), prepared, None).unwrap();
    assert_eq!(stream.read(start, 1, 1_000_000).unwrap().batches.len(), 1);
    drop(stream);
    let reopened = ChangeStreamManager::open(path.clone(), ChangeStorageKind::Heap, StorageId(5), &table,
        |txn| if txn == TxnId(1) && std::fs::read(&decision).ok().as_deref() == Some(&1_u64.to_le_bytes()) {
            AuthoritativeOutcome::Committed(None)
        } else {
            AuthoritativeOutcome::Unresolved
        }).unwrap();
    assert_eq!(reopened.read(start, 1, 1_000_000).unwrap().batches.len(), 1);
    assert!(reopened.read(start, 0, 1_000_000).unwrap().batches.is_empty());
}
''',
"lsm": r'''
use netbadb_lsm::LsmStorage;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("CASE_PATH").unwrap());
    let table = TableDef::new(TableId(1), "rows", vec![ColumnDef::new(ColumnId(1), "n", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut storage = LsmStorage::create(&path, table.clone(), ColumnId(1)).unwrap();
    let mut committed = storage.begin_transaction().unwrap();
    storage.insert_in(&mut committed, &[ScalarValue::Int64(7)]).unwrap();
    committed.commit().unwrap();
    let mut aborted = storage.begin_transaction().unwrap();
    storage.insert_in(&mut aborted, &[ScalarValue::Int64(8)]).unwrap();
    aborted.rollback().unwrap();
    let view = storage.read_view().unwrap();
    assert_eq!(storage.scan_columns_with_view(&[ColumnId(1)], &view).unwrap().len(), 1);
    drop(view);
    storage.close().unwrap();
    let mut reopened = LsmStorage::open(&path, table).unwrap();
    let view = reopened.read_view().unwrap();
    assert_eq!(reopened.scan_columns_with_view(&[ColumnId(1)], &view).unwrap()[0].1, vec![ScalarValue::Int64(7)]);
}
''',
"columnar": r'''
use netbadb_columnar::{ColumnarProjection, StorageSnapshotToken};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, PhysicalType, ScalarValue, StorageId, TableId};
fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("CASE_PATH").unwrap());
    let table = TableDef::new(TableId(1), "rows", vec![ColumnDef::new(ColumnId(1), "n", TypeSpec::Physical(PhysicalType::Int64))]);
    ColumnarProjection::prepare(&path, ColumnarProjectionId(1), ColumnarGeneration(1), &table,
        StorageId(5), StorageSnapshotToken::heap(StorageId(5), 1), &[ColumnId(1)],
        &[vec![ScalarValue::Int64(7)]], None).unwrap().publish().unwrap();
    let reopened = ColumnarProjection::open(&path, &table).unwrap();
    let (batches, _) = reopened.scan(&[ColumnId(1)], &[]).unwrap();
    assert_eq!(batches.iter().map(|batch| batch.row_count).sum::<usize>(), 1);
}
''',
"heap": r'''
use netbadb_heap::HeapStorage;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
fn main() {
    let path = std::path::PathBuf::from(std::env::var_os("CASE_PATH").unwrap());
    let table = TableDef::new(TableId(1), "rows", vec![ColumnDef::new(ColumnId(1), "n", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut storage = HeapStorage::create(&path, table.clone()).unwrap();
    storage.create_index(ColumnId(1)).unwrap();
    assert!(storage.index_for_column(ColumnId(1)).is_some());
    let mut committed = storage.begin_transaction().unwrap();
    storage.insert_in(&mut committed, &[ScalarValue::Int64(7)]).unwrap();
    committed.commit().unwrap();
    let mut aborted = storage.begin_transaction().unwrap();
    storage.insert_in(&mut aborted, &[ScalarValue::Int64(8)]).unwrap();
    aborted.rollback().unwrap();
    assert_eq!(storage.scan().unwrap().len(), 1);
    storage.close().unwrap();
    let mut reopened = HeapStorage::open(&path, table).unwrap();
    assert!(reopened.index_for_column(ColumnId(1)).is_some());
    assert_eq!(reopened.scan().unwrap()[0].1, vec![ScalarValue::Int64(7)]);
}
''',
"query-feedback": r'''
use netbadb_planner::{PlanNodeOrdinal, PlannerAccessEstimate, PlannerAccessKind, PlannerCalibrationEpoch, PlannerWorkModel, PlanVariant};
use netbadb_query_feedback::{correlate_execution_feedback, ExecutionAccessKind, ExecutionAccessSample, ExecutionFeedbackAnchor, ExecutionStatistics, ExecutionWork};
use netbadb_rel::LogicalQueryShape;
use netbadb_types::{RelationBindingId, SchemaGeneration, StorageId, TableId};
fn main() {
    let estimate = PlannerAccessEstimate { node: PlanNodeOrdinal(1), binding_id: RelationBindingId(1), table_id: TableId(1),
        storage_id: Some(StorageId(3)), kind: PlannerAccessKind::SeqScan, access_path: None,
        projection_id: None, projection_generation: None, estimated_work_units: Some(4), effective_work_units: None,
        calibration_epoch: PlannerCalibrationEpoch(1), source_alternative_work_units: None,
        effective_source_alternative_work_units: None, work_model: PlannerWorkModel::SequentialRows };
    let actual = ExecutionAccessSample { node: PlanNodeOrdinal(1), binding_id: RelationBindingId(1), table_id: TableId(1),
        storage_id: StorageId(3), partition_id: None, kind: ExecutionAccessKind::SeqScan, access_path: None,
        work: ExecutionWork { rows_examined: 7, ..Default::default() } };
    let anchor = ExecutionFeedbackAnchor { global_commit_seq: None, schema_generation: SchemaGeneration(1) };
    let report = correlate_execution_feedback(anchor, PlannerCalibrationEpoch(1), LogicalQueryShape::OneRow,
        PlanVariant::OneRow, &[estimate.clone()], ExecutionStatistics { accesses: vec![actual.clone()], ..Default::default() });
    assert_eq!(report.accesses.len(), 1);
    assert!(report.accesses[0].planner.is_some());
    assert!(report.accesses[0].calibration.is_some());
    let invalid = ExecutionAccessSample { storage_id: StorageId(4), ..actual };
    let report = correlate_execution_feedback(anchor, PlannerCalibrationEpoch(1), LogicalQueryShape::OneRow,
        PlanVariant::OneRow, &[estimate], ExecutionStatistics { accesses: vec![invalid], ..Default::default() });
    assert!(report.accesses[0].planner.is_none());
    assert!(report.accesses[0].calibration.is_none());
}
''',
"advisor": r'''
use netbadb_advisor::{candidate, plan_maintenance, evaluate_scheduler_gate, AdaptiveEvidenceProgressToken,
    AutomaticSchedulerInspection, AutomaticSchedulerPolicy, AutomaticSchedulerState, AutomaticSchedulerTick,
    MaintenanceAction, MaintenanceBound, MaintenanceBlocker, MaintenanceBudget, MaintenanceEstimate, MaintenanceReason,
    AdaptiveLsmMaintenanceObservation, AdaptiveLsmMaintenanceDecision, AdaptiveLsmFlushPolicy, revalidate_lsm_proposal};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SchemaGeneration, StorageDataVersion, StorageId, TableId};
use netbadb_lsm::LsmStorage;
use netbadb_columnar::StorageSnapshotToken;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage_api::StorageKind;
fn main() {
    let flush = MaintenanceAction::FlushLsm { table_id: TableId(1), storage_id: StorageId(1) };
    let compact = MaintenanceAction::CompactLsm { table_id: TableId(1), storage_id: StorageId(1) };
    let estimate = MaintenanceEstimate { work_units: 5, read_bytes: 3, write_bytes: 4 };
    let budget = MaintenanceBudget::new(10, 10, 10, 1);
    let good = candidate(flush, MaintenanceReason::LsmMemtableFlush, MaintenanceBound::EstimateGatedAtomic,
        estimate, None, false, budget);
    let rejected = candidate(compact, MaintenanceReason::LsmCompactionPressure, MaintenanceBound::EstimateGatedAtomic,
        estimate, None, false, MaintenanceBudget::new(0, 10, 10, 1));
    assert_eq!(rejected.blocker, Some(MaintenanceBlocker::WorkBudgetExceeded));
    let ranked = plan_maintenance(vec![rejected, good], None);
    assert_eq!(ranked.decision.unwrap().action, flush);
    let path = std::path::PathBuf::from(std::env::var_os("CASE_PATH").unwrap());
    let table = TableDef::new(TableId(1), "rows", vec![ColumnDef::new(ColumnId(1), "n", TypeSpec::Physical(PhysicalType::Int64))]);
    let mut storage = LsmStorage::create(&path, table, ColumnId(1)).unwrap();
    storage.insert(&[ScalarValue::Int64(7)]).unwrap();
    let lsm = storage.inspection();
    let mut maintenance = storage.maintenance_inspection().unwrap();
    let change_stream = storage.change_stream_inspection();
    let (epoch, sequence) = storage.projection_snapshot_parts().unwrap();
    let storage_id = storage.storage_id();
    let estimate = maintenance.flush_cost.unwrap();
    let production = candidate(flush, MaintenanceReason::LsmMemtableFlush,
        MaintenanceBound::EstimateGatedAtomic, MaintenanceEstimate {
            work_units: estimate.work_units, read_bytes: estimate.read_bytes, write_bytes: estimate.write_bytes,
        }, None, false, MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1));
    // Synthetic eligible observation for the pure proposal API; no engine state is changed.
    maintenance.memtable_flush_threshold_bytes = maintenance.memtable_bytes;
    let observation = AdaptiveLsmMaintenanceObservation {
        observed_global_commit_seq: None, schema_generation: SchemaGeneration(1), table_id: TableId(1),
        storage_id, storage_kind: StorageKind::Lsm,
        storage_snapshot: StorageSnapshotToken::lsm(storage_id, epoch, sequence),
        logical_data_version: StorageDataVersion(1), lsm, maintenance, change_stream,
        production_flush_candidate: Some(production), production_compaction_candidate: None,
    };
    let generous = MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1);
    let proposal = match observation.decide_flush(AdaptiveLsmFlushPolicy { minimum_memtable_bytes: 0 }, generous) {
        AdaptiveLsmMaintenanceDecision::Proposal(proposal) => proposal,
        other => panic!("expected proposal: {other:?}"),
    };
    assert!(revalidate_lsm_proposal(&proposal, &observation, generous));
    let mut stale = observation.clone();
    stale.logical_data_version = StorageDataVersion(2);
    assert!(!revalidate_lsm_proposal(&proposal, &stale, generous));
    let policy = AutomaticSchedulerPolicy::new(1, 2, 2, 3).unwrap();
    let state = AutomaticSchedulerState::INITIAL;
    let evidence = AdaptiveEvidenceProgressToken::default();
    assert_eq!(evaluate_scheduler_gate(state, policy, evidence, AutomaticSchedulerTick(1)).unwrap(),
        AutomaticSchedulerInspection::WouldRunNow);
}
''',
}


def run(command, **kwargs):
    subprocess.run(command, check=True, **kwargs)


def main():
    print(f"consumers: source={subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()} toolchain={subprocess.check_output(['rustc', '--version'], text=True).strip()}", flush=True)
    with tempfile.TemporaryDirectory(prefix="netbadb-minimal-") as temporary:
        root = Path(temporary)
        for name, basics in BASE.items():
            project = root / name
            (project / "src").mkdir(parents=True)
            dependencies = [name, *basics]
            manifest = '[package]\nname = "netbadb-consumer-' + name + '"\nversion = "0.0.0"\nedition = "2024"\n\n[dependencies]\n'
            manifest += ''.join(f'netbadb-{dependency} = {{ path = "{REPO / "crates" / ("netbadb-" + dependency)}" }}\n' for dependency in dependencies)
            (project / "Cargo.toml").write_text(manifest)
            (project / "src/main.rs").write_text(SOURCES[name])
            metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--offline", "--format-version", "1", "--manifest-path", str(project / "Cargo.toml")], text=True))
            packages = {p["id"]: p for p in metadata["packages"]}
            root_id = metadata["resolve"]["root"]
            nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
            seen, pending = set(), [root_id]
            while pending:
                identity = pending.pop()
                if identity in seen:
                    continue
                seen.add(identity)
                for edge in nodes[identity]["deps"]:
                    if any((kind["kind"] or "normal") in ("normal", "build") for kind in edge["dep_kinds"]):
                        pending.append(edge["pkg"])
            names = {packages[i]["name"] for i in seen}
            forbidden = names & {"netbadb-core", "netbadb-server", "netbadb-executor", "netbadb-storage"}
            if forbidden:
                raise RuntimeError(f"{name} pulls forbidden composition dependencies: {sorted(forbidden)}")
            if any("test-hooks" in n.get("features", []) for n in nodes.values() if n["id"] in seen):
                raise RuntimeError(f"{name} enabled test-hooks")
            print(f"{name}: normal/build graph checked ({len(seen)} packages), running", flush=True)
            env = os.environ.copy()
            env["CARGO_TARGET_DIR"] = str(REPO / "target")
            env["CASE_PATH"] = str(root / (name + "-data"))
            run(["cargo", "run", "--quiet", "--offline", "--manifest-path", str(project / "Cargo.toml")], env=env)
    print("Seven minimal consumers passed", flush=True)


if __name__ == "__main__":
    main()
