//! Phase 3B.5 end-to-end proofs for explicit coordinator checkpoint compaction.

use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, DatabaseCommitSeq, PhysicalType, ScalarValue, TableId};
use std::path::{Path, PathBuf};

const ITEMS: TableId = TableId(1);

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-phase3b5-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn create(path: &Path, global: bool) -> Database {
    let coordinator = DatabaseCoordinatorConfig::new(path.join("coordinator"));
    let coordinator = if global {
        coordinator.with_global_visibility()
    } else {
        coordinator
    };
    Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("items.heap"),
            TableDef::new(
                ITEMS,
                "items",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(coordinator),
    )
    .unwrap()
}

fn append_rows(database: &mut Database, start: i64, count: i64) {
    for id in start..start + count {
        database
            .execute(&format!("INSERT INTO items VALUES ({id})"))
            .unwrap();
    }
}

fn directory_bytes(path: &Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (
                path.file_name().unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

#[test]
fn explicit_compaction_preserves_visibility_identity_and_constant_size() {
    let path = root("end-to-end");
    let mut database = create(&path, true);
    append_rows(&mut database, 1, 100);
    let before_snapshot = database.current_database_snapshot().unwrap().unwrap();
    let next_transaction_id_before = database.next_transaction_id;
    let before = database.inspect_global_visibility().unwrap();
    assert_eq!(before.published_commit_seq, Some(DatabaseCommitSeq(100)));
    assert_eq!(before.retained_decision_count, 100);
    assert!(before.compaction_possible);
    assert_eq!(before.pending_complete_count, 1);

    let report = database.compact_coordinator_log().unwrap();
    assert!(report.compacted);
    assert_eq!(report.checkpointed_through, DatabaseCommitSeq(100));
    assert_eq!(report.decisions_before, 100);
    assert_eq!(report.decisions_after, 0);
    assert_eq!(report.decisions_compacted, 100);
    assert_eq!(report.pending_completes_flushed, 1);
    assert!(report.bytes_reclaimed > 0);
    assert_eq!(report.bytes_after, 104);
    assert_eq!(
        database.current_database_snapshot().unwrap(),
        Some(before_snapshot)
    );
    assert_eq!(database.next_transaction_id, next_transaction_id_before);

    let second = database.compact_coordinator_log().unwrap();
    assert!(!second.compacted);
    assert_eq!(second.bytes_before, second.bytes_after);
    database.close().unwrap();

    let mut database = Database::open_catalog(path.join("catalog")).unwrap();
    let reopened = database.inspect_global_visibility().unwrap();
    assert_eq!(reopened.checkpointed_through, Some(DatabaseCommitSeq(100)));
    assert_eq!(reopened.published_commit_seq, Some(DatabaseCommitSeq(100)));
    assert_eq!(reopened.next_commit_seq, Some(DatabaseCommitSeq(101)));
    assert_eq!(reopened.retained_decision_count, 0);
    let transaction = database.begin_transaction().unwrap();
    assert!(transaction.id().0 > report.database_txn_id_high_water.0);
    let next_transaction_id = transaction.id();
    drop(transaction);
    database.execute("INSERT INTO items VALUES (101)").unwrap();
    let continued = database.inspect_global_visibility().unwrap();
    assert_eq!(continued.published_commit_seq, Some(DatabaseCommitSeq(101)));
    assert_eq!(continued.retained_decision_count, 1);
    assert!(database.next_transaction_id.0 > next_transaction_id.0);
    assert_eq!(
        database
            .query("SELECT id FROM items ORDER BY id")
            .unwrap()
            .rows
            .len(),
        101
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn compaction_is_global_quiescent_and_preserves_structural_evidence() {
    let legacy_path = root("legacy");
    let mut legacy = create(&legacy_path, false);
    assert!(matches!(
        legacy.compact_coordinator_log(),
        Err(DatabaseError::Transaction(
            CoordinatorError::CoordinatorCompactionRequiresGlobalVisibility
        ))
    ));
    legacy.close().unwrap();
    std::fs::remove_dir_all(legacy_path).unwrap();

    let empty_path = root("empty");
    let mut empty = create(&empty_path, true);
    let no_work = empty.compact_coordinator_log().unwrap();
    assert!(!no_work.compacted);
    assert_eq!(no_work.checkpointed_through, DatabaseCommitSeq(0));
    empty.close().unwrap();
    std::fs::remove_dir_all(empty_path).unwrap();

    let busy_path = root("busy");
    let mut busy = create(&busy_path, true);
    append_rows(&mut busy, 1, 1);
    let repeatable = busy
        .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
        .unwrap();
    assert!(matches!(
        busy.compact_coordinator_log(),
        Err(DatabaseError::Transaction(
            CoordinatorError::CoordinatorCompactionRequiresQuiescence { outstanding: 1 }
        ))
    ));
    drop(repeatable);
    busy.compact_coordinator_log().unwrap();
    busy.close().unwrap();
    std::fs::remove_dir_all(busy_path).unwrap();

    let structural_path = root("structural-evidence");
    let mut structural = create(&structural_path, true);
    append_rows(&mut structural, 1, 1);
    structural
        .execute("CREATE TABLE later (id BIGINT NOT NULL)")
        .unwrap();
    let before = structural.inspect_global_visibility().unwrap();
    assert!(!before.compaction_possible);
    assert!(matches!(
        structural.compact_coordinator_log(),
        Err(DatabaseError::Transaction(
            CoordinatorError::CoordinatorCompactionStructuralHistoryRequired
        ))
    ));
    structural.close().unwrap();
    let reopened = Database::open_catalog(structural_path.join("catalog")).unwrap();
    assert!(reopened.schema().table("later").is_some());
    reopened.close().unwrap();
    std::fs::remove_dir_all(structural_path).unwrap();
}

#[test]
fn checkpoint_requires_tail_evidence_for_any_still_prepared_participant() {
    let prepared = netbadb_storage::PreparedTransaction {
        database_txn_id: netbadb_types::DatabaseTxnId(9),
        physical_txn_id: netbadb_types::TxnId(90),
        state: netbadb_storage::PreparedTransactionState::Prepared,
    };
    assert!(matches!(
        resolution_for_recovery_prepared(
            &prepared,
            netbadb_types::StorageId(3),
            &[],
            Some(netbadb_types::DatabaseTxnId(10)),
        ),
        Err(DatabaseError::CompactedPreparedParticipant {
            database_txn_id: netbadb_types::DatabaseTxnId(9),
            checkpoint_high_water: netbadb_types::DatabaseTxnId(10),
            ..
        })
    ));
}

#[test]
fn schema_commit_after_checkpoint_uses_the_next_global_sequence() {
    let path = root("schema-after");
    let mut database = create(&path, true);
    append_rows(&mut database, 1, 10);
    database.compact_coordinator_log().unwrap();
    database
        .execute("CREATE TABLE after_checkpoint (id BIGINT NOT NULL)")
        .unwrap();
    assert_eq!(
        database
            .inspect_global_visibility()
            .unwrap()
            .published_commit_seq,
        Some(DatabaseCommitSeq(11))
    );
    database.close().unwrap();
    let reopened = Database::open_catalog(path.join("catalog")).unwrap();
    assert!(reopened.schema().table("after_checkpoint").is_some());
    assert_eq!(
        reopened
            .inspect_global_visibility()
            .unwrap()
            .published_commit_seq,
        Some(DatabaseCommitSeq(11))
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn coordinator_compaction_does_not_touch_fresh_columnar_state() {
    let path = root("columnar-independent");
    let projection = path.join("projection");
    let mut database = create(&path, true);
    append_rows(&mut database, 1, 20);
    let projection_id = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            ITEMS,
            &projection,
            vec![ColumnId(1)],
        ))
        .unwrap();
    let sql = "SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM items";
    let before_result = database.query(sql).unwrap();
    let before_projection = database.inspect_columnar_projections()[0].clone();
    let before_files = directory_bytes(&projection);
    let before_snapshot = database.current_database_snapshot().unwrap();

    database.compact_coordinator_log().unwrap();

    assert_eq!(
        database.current_database_snapshot().unwrap(),
        before_snapshot
    );
    assert_eq!(database.query(sql).unwrap(), before_result);
    assert_eq!(
        database.inspect_columnar_projections()[0],
        before_projection
    );
    assert_eq!(directory_bytes(&projection), before_files);
    assert_eq!(
        database.inspect_columnar_projections()[0].projection_id,
        Some(projection_id)
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn coordinator_compaction_crash_child() {
    let Ok(root) = std::env::var("NETBADB_PHASE3B5_ROOT") else {
        return;
    };
    let mut database = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    match std::env::var("NETBADB_PHASE3B5_CHILD_MODE").as_deref() {
        Ok("compact") => {
            database.compact_coordinator_log().unwrap();
        }
        Ok("write") => {
            database.execute("INSERT INTO items VALUES (6)").unwrap();
        }
        mode => panic!("unknown crash child mode {mode:?}"),
    }
    panic!("configured Phase 3B.5 crash point was not reached");
}

fn crash_child(path: &Path, point: &str, mode: &str) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "coordinator_compaction_tests::coordinator_compaction_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_PHASE3B5_ROOT", path)
        .env("NETBADB_PHASE3B5_CHILD_MODE", mode);
    crate::coordinator_crash::configure_child(&mut command, point, path, point);
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(crate::coordinator_crash::EXIT_CODE),
        "{point}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn compaction_crash_matrix_reopens_only_old_or_checkpointed_authority() {
    for (point, checkpointed) in [
        ("coordinator-compact-before-temp-create", false),
        ("coordinator-compact-during-temp-write", false),
        ("coordinator-compact-after-temp-write", false),
        ("coordinator-compact-after-temp-sync", false),
        ("coordinator-compact-after-handle-release", false),
        ("coordinator-compact-after-rename", true),
        ("coordinator-compact-after-directory-sync", true),
        ("coordinator-compact-after-state-replacement", true),
    ] {
        let path = root(&format!("crash-{point}"));
        let mut database = create(&path, true);
        append_rows(&mut database, 1, 5);
        database.close().unwrap();
        crash_child(&path, point, "compact");
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
            let inspection = reopened.inspect_global_visibility().unwrap();
            assert_eq!(inspection.published_commit_seq, Some(DatabaseCommitSeq(5)));
            assert_eq!(inspection.next_commit_seq, Some(DatabaseCommitSeq(6)));
            assert_eq!(inspection.checkpointed_through.is_some(), checkpointed);
            assert_eq!(
                inspection.retained_decision_count,
                if checkpointed { 0 } else { 5 }
            );
            assert_eq!(
                reopened
                    .query("SELECT id FROM items ORDER BY id")
                    .unwrap()
                    .rows,
                (1..=5)
                    .map(|id| vec![ScalarValue::Int64(id)])
                    .collect::<Vec<_>>()
            );
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn post_checkpoint_deferred_complete_and_partial_decision_recover() {
    for (point, winner) in [
        ("after-deferred-complete-before-publication", true),
        ("during-decision-append", false),
    ] {
        let path = root(&format!("tail-{point}"));
        let mut database = create(&path, true);
        append_rows(&mut database, 1, 5);
        database.compact_coordinator_log().unwrap();
        database.close().unwrap();
        crash_child(&path, point, "write");
        for _ in 0..3 {
            let reopened = Database::open_catalog(path.join("catalog")).unwrap();
            let inspection = reopened.inspect_global_visibility().unwrap();
            assert_eq!(
                inspection.published_commit_seq,
                Some(DatabaseCommitSeq(if winner { 6 } else { 5 }))
            );
            assert_eq!(
                inspection.next_commit_seq,
                Some(DatabaseCommitSeq(if winner { 7 } else { 6 }))
            );
            assert_eq!(inspection.checkpointed_through, Some(DatabaseCommitSeq(5)));
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
