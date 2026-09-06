use std::path::PathBuf;

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    ChangeStreamError, StorageChange, StorageError, StorageVersionKey, TableStorage,
    heap_change_log_path,
};
use netbadb_types::{ColumnId, DatabaseTxnId, PhysicalType, ScalarValue, StorageId, TableId};

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "netbadb-change-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn table() -> TableDef {
    TableDef::new(
        TableId(7),
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "key", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "value", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn row(key: i64, value: &str) -> Vec<ScalarValue> {
    vec![ScalarValue::Int64(key), ScalarValue::Text(value.into())]
}

fn cleanup_heap(path: &std::path::Path) {
    for component in netbadb_storage::heap_resource_components(path) {
        let _ = std::fs::remove_file(component.path);
    }
}

#[test]
fn heap_change_chain_coalesces_rollbacks_reopens_and_rejects_old_incarnation() {
    let path = path("heap").with_extension("db");
    cleanup_heap(&path);
    let mut storage = TableStorage::create_heap_with_storage_id(&path, table(), StorageId(11))
        .expect("create heap");
    let baseline = storage.enable_change_stream().expect("enable stream");
    let anchor = storage.committed_read_anchor().expect("capture anchor");

    let first = storage.insert(&row(1, "a")).expect("insert");
    assert!(
        storage
            .scan_columns_with_view(&[ColumnId(1)], &anchor.read_view)
            .expect("scan anchored view")
            .is_empty()
    );
    let after_anchor = storage
        .read_changes(anchor.cursor, 10, 1_000_000)
        .expect("changes after anchor");
    assert_eq!(after_anchor.batches.len(), 1);
    drop(anchor);
    let second = storage.update(first, &row(1, "b")).expect("update");
    storage.delete(second).expect("delete");

    let result = storage
        .read_changes(baseline, 10, 1_000_000)
        .expect("read changes");
    assert_eq!(result.batches.len(), 3);
    assert!(!result.has_more);
    assert_eq!(result.batches[0].before, baseline.frontier);
    assert_eq!(result.batches[0].after, result.batches[1].before);
    assert_eq!(result.batches[1].after, result.batches[2].before);
    assert!(
        matches!(result.batches[0].mutations.as_slice(), [StorageChange::Insert { after, .. }] if after == &row(1, "a"))
    );
    assert!(
        matches!(result.batches[1].mutations.as_slice(), [StorageChange::Update { old_version: StorageVersionKey::Heap { .. }, new_version: StorageVersionKey::Heap { .. }, after }] if after == &row(1, "b"))
    );
    assert!(matches!(
        result.batches[2].mutations.as_slice(),
        [StorageChange::Delete { .. }]
    ));

    let frontier = result.current_frontier;
    let mut rollback = storage.begin_transaction().expect("begin rollback");
    storage
        .insert_in(&mut rollback, &row(2, "rollback"))
        .expect("pending insert");
    rollback.rollback().expect("rollback");
    assert_eq!(storage.change_stream_cursor().unwrap().frontier, frontier);

    let mut prepared_abort = storage.begin_transaction().expect("begin prepared abort");
    storage
        .insert_in(&mut prepared_abort, &row(8, "prepared abort"))
        .expect("pending prepared insert");
    prepared_abort
        .prepare(DatabaseTxnId(81))
        .expect("durably prepare");
    prepared_abort
        .rollback_prepared(DatabaseTxnId(81))
        .expect("durably abort prepared transaction");
    assert_eq!(storage.change_stream_cursor().unwrap().frontier, frontier);

    let coalesce_cursor = storage.change_stream_cursor().unwrap();
    let mut transaction = storage.begin_transaction().expect("begin coalesce");
    let inserted = storage
        .insert_in(&mut transaction, &row(3, "first"))
        .expect("insert in");
    let mut updated = inserted;
    for revision in 0..100 {
        updated = storage
            .update_in(
                &mut transaction,
                updated,
                &row(3, &format!("revision-{revision}")),
            )
            .expect("update in");
    }
    transaction.commit().expect("commit coalesced");
    let coalesced = storage
        .read_changes(coalesce_cursor, 10, 1_000_000)
        .expect("read coalesced");
    assert!(
        matches!(coalesced.batches[0].mutations.as_slice(), [StorageChange::Insert { after, .. }] if after == &row(3, "revision-99"))
    );

    let update_cursor = storage.change_stream_cursor().unwrap();
    let mut transaction = storage.begin_transaction().expect("begin repeated updates");
    for revision in 0..100 {
        updated = storage
            .update_in(
                &mut transaction,
                updated,
                &row(3, &format!("committed-{revision}")),
            )
            .expect("repeat update");
    }
    transaction.commit().expect("commit repeated updates");
    let repeated = storage
        .read_changes(update_cursor, 10, 1_000_000)
        .expect("read repeated update");
    assert!(
        matches!(repeated.batches[0].mutations.as_slice(), [StorageChange::Update { after, .. }] if after == &row(3, "committed-99"))
    );

    let no_op_cursor = storage.change_stream_cursor().unwrap();
    let mut transaction = storage.begin_transaction().expect("begin insert delete");
    let transient = storage
        .insert_in(&mut transaction, &row(4, "transient"))
        .expect("insert transient");
    storage
        .delete_in(&mut transaction, transient)
        .expect("delete transient");
    transaction.commit().expect("commit net no-op");
    assert_eq!(storage.change_stream_cursor().unwrap(), no_op_cursor);

    let delete_cursor = storage.change_stream_cursor().unwrap();
    let mut transaction = storage.begin_transaction().expect("begin update delete");
    let final_version = storage
        .update_in(&mut transaction, updated, &row(3, "doomed"))
        .expect("update before delete");
    storage
        .delete_in(&mut transaction, final_version)
        .expect("delete after update");
    transaction.commit().expect("commit update delete");
    let deleted = storage
        .read_changes(delete_cursor, 10, 1_000_000)
        .expect("read update delete");
    assert!(matches!(
        deleted.batches[0].mutations.as_slice(),
        [StorageChange::Delete { .. }]
    ));

    storage.close().expect("close");
    let change_path = heap_change_log_path(&path);
    let change_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&change_path)
        .expect("open change log for crash-tail simulation");
    let length = change_file.metadata().expect("change metadata").len();
    change_file
        .set_len(length - 1)
        .expect("truncate final publication marker");
    drop(change_file);
    let mut reopened = TableStorage::open_heap(&path, table()).expect("reopen");
    let replay = reopened
        .read_changes(baseline, 10, 1_000_000)
        .expect("replay after reopen");
    assert_eq!(replay.batches.len(), 6);
    reopened.disable_change_stream().expect("disable");
    let next = reopened.enable_change_stream().expect("re-enable");
    assert_ne!(baseline.generation, next.generation);
    assert!(matches!(
        reopened.read_changes(baseline, 1, 1_000),
        Err(StorageError::ChangeStream(
            ChangeStreamError::StreamIdentityMismatch
        ))
    ));
    reopened.close().expect("close reopened");
    cleanup_heap(&path);
}

#[test]
fn gc_persists_retained_and_current_frontiers_and_sequence_high_water() {
    let path = path("gc-frontiers").with_extension("db");
    cleanup_heap(&path);
    let mut storage = TableStorage::create_heap_with_storage_id(&path, table(), StorageId(44))
        .expect("create heap");
    let origin = storage.enable_change_stream().expect("enable stream");
    storage.insert(&row(1, "a")).expect("insert one");
    storage.insert(&row(2, "b")).expect("insert two");
    storage.insert(&row(3, "c")).expect("insert three");
    let all = storage
        .read_changes(origin, 10, 1_000_000)
        .expect("read complete history");
    assert_eq!(all.batches.len(), 3);
    let maintenance_before = storage.inspect_change_stream_maintenance();
    assert_eq!(maintenance_before.batches.len(), 3);
    assert!(
        maintenance_before.batches.iter().all(|batch| {
            batch.change_bytes > 0 && batch.retained_file_bytes > batch.change_bytes
        })
    );
    let retained = all.batches[1].after;
    let current = all.current_frontier;

    let report = storage
        .gc_change_stream(retained)
        .expect("retain final batch");
    assert_eq!(report.previous_earliest_frontier, origin.frontier);
    assert_eq!(report.new_earliest_frontier, retained);
    assert_eq!(report.current_frontier, current);
    assert_eq!(report.batches_removed, 2);
    assert!(report.bytes_after < report.bytes_before);
    let inspection = storage.inspect_change_stream();
    assert_eq!(inspection.stream_origin_frontier, Some(origin.frontier));
    assert_eq!(inspection.baseline_data_version, Some(origin.frontier));
    assert_eq!(inspection.earliest_available_frontier, Some(retained));
    assert_eq!(inspection.current_data_version, current);
    let maintenance_after = storage.inspect_change_stream_maintenance();
    assert_eq!(maintenance_after.batches.len(), 1);
    assert_eq!(maintenance_after.batches[0].before, retained);
    let old = netbadb_storage::ChangeStreamCursor {
        frontier: netbadb_types::StorageDataVersion(retained.0 - 1),
        ..origin
    };
    assert!(matches!(
        storage.read_changes(old, 10, 1_000_000),
        Err(StorageError::ChangeStream(
            ChangeStreamError::HistoryUnavailable
        ))
    ));
    let retained_cursor = netbadb_storage::ChangeStreamCursor {
        frontier: retained,
        ..origin
    };
    let tail = storage
        .read_changes(retained_cursor, 10, 1_000_000)
        .expect("read retained history");
    assert_eq!(tail.batches.len(), 1);
    assert_eq!(tail.batches[0].sequence, 3);

    storage
        .gc_change_stream(current)
        .expect("remove every committed batch");
    storage.close().expect("close after GC");
    let mut reopened = TableStorage::open_heap(&path, table()).expect("reopen empty retained log");
    let reopened_inspection = reopened.inspect_change_stream();
    assert_eq!(
        reopened_inspection.stream_origin_frontier,
        Some(origin.frontier)
    );
    assert_eq!(
        reopened_inspection.earliest_available_frontier,
        Some(current)
    );
    assert_eq!(reopened_inspection.current_data_version, current);
    assert_eq!(reopened_inspection.committed_batch_count, 0);
    assert!(
        reopened
            .inspect_change_stream_maintenance()
            .batches
            .is_empty()
    );

    reopened.insert(&row(4, "d")).expect("post-GC insert");
    let post_gc_cursor = netbadb_storage::ChangeStreamCursor {
        frontier: current,
        ..origin
    };
    let post_gc = reopened
        .read_changes(post_gc_cursor, 10, 1_000_000)
        .expect("read post-GC batch");
    assert_eq!(post_gc.batches.len(), 1);
    assert_eq!(post_gc.batches[0].before, current);
    assert_eq!(post_gc.batches[0].after.0, current.0 + 1);
    assert_eq!(post_gc.batches[0].sequence, 4);
    let maintenance_reopened = reopened.inspect_change_stream_maintenance();
    assert_eq!(maintenance_reopened.batches.len(), 1);
    assert_eq!(maintenance_reopened.batches[0].before, current);
    reopened.close().expect("close reopened heap");
    let change_path = heap_change_log_path(&path);
    let change_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&change_path)
        .expect("open post-GC log");
    let length = change_file.metadata().expect("post-GC metadata").len();
    change_file
        .set_len(length - 1)
        .expect("truncate post-GC finalize tail");
    drop(change_file);
    let recovered = TableStorage::open_heap(&path, table()).expect("recover post-GC finalize");
    let recovered_batch = recovered
        .read_changes(post_gc_cursor, 10, 1_000_000)
        .expect("read recovered post-GC batch");
    assert_eq!(recovered_batch.batches.len(), 1);
    assert_eq!(recovered_batch.batches[0].sequence, 4);
    recovered.close().expect("close recovered heap");
    cleanup_heap(&path);
}

#[test]
fn lsm_stream_preserves_logical_updates_across_flush_compaction_and_reopen() {
    let root = path("lsm");
    let _ = std::fs::remove_dir_all(&root);
    let mut storage =
        TableStorage::create_lsm_with_storage_id(&root, table(), ColumnId(1), StorageId(22))
            .expect("create LSM");
    let baseline = storage.enable_change_stream().expect("enable");
    let mut rollback = storage.begin_transaction().expect("begin rollback");
    storage
        .insert_in(&mut rollback, &row(5, "rollback"))
        .expect("pending LSM insert");
    rollback.rollback().expect("rollback LSM");
    assert_eq!(storage.change_stream_cursor().unwrap(), baseline);

    let mut no_op = storage.begin_transaction().expect("begin net no-op");
    let transient = storage
        .insert_in(&mut no_op, &row(6, "transient"))
        .expect("insert transient");
    storage
        .delete_in(&mut no_op, transient)
        .expect("delete transient");
    no_op.commit().expect("commit net no-op");
    assert_eq!(storage.change_stream_cursor().unwrap(), baseline);
    let first = storage.insert(&row(10, "a")).expect("insert");
    let moved = storage.update(first, &row(20, "b")).expect("move key");
    storage.flush().expect("flush");
    storage.compact_full().expect("compact");
    storage.delete(moved).expect("delete");
    let before_close = storage.read_changes(baseline, 10, 1_000_000).expect("read");
    assert_eq!(before_close.batches.len(), 3);
    assert!(
        matches!(before_close.batches[1].mutations.as_slice(), [StorageChange::Update { old_version: StorageVersionKey::Lsm { version: old, .. }, new_version: StorageVersionKey::Lsm { version: new, .. }, after }] if old.0 > 0 && new.0 > old.0 && after == &row(20, "b"))
    );
    storage.close().expect("close");

    let reopened = TableStorage::open_lsm(&root, table()).expect("reopen");
    let replay = reopened
        .read_changes(baseline, 10, 1_000_000)
        .expect("replay");
    assert_eq!(replay.batches, before_close.batches);
    assert_eq!(replay.current_frontier, before_close.current_frontier);
    reopened.close().expect("close reopened");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn corrupt_active_stream_keeps_reads_available_and_blocks_changing_commit() {
    let path = path("unavailable").with_extension("db");
    cleanup_heap(&path);
    let mut storage = TableStorage::create_heap_with_storage_id(&path, table(), StorageId(33))
        .expect("create heap");
    storage.insert(&row(1, "before")).expect("seed row");
    storage.enable_change_stream().expect("enable");
    storage.close().expect("close");

    let change_path = netbadb_storage::heap_change_log_path(&path);
    let mut bytes = std::fs::read(&change_path).expect("read change log");
    bytes[76] ^= 0x80;
    std::fs::write(&change_path, bytes).expect("corrupt checksum");

    let mut reopened = TableStorage::open_heap(&path, table()).expect("authoritative reopen");
    let view = reopened.read_view().expect("read view");
    assert_eq!(
        reopened
            .scan_columns_with_view(&[ColumnId(1), ColumnId(2)], &view)
            .expect("authoritative read")
            .into_iter()
            .map(|(_, values)| values)
            .collect::<Vec<_>>(),
        vec![row(1, "before")]
    );
    drop(view);
    assert_eq!(
        reopened.inspect_change_stream().status,
        netbadb_storage::ChangeStreamStatus::Unavailable
    );
    assert!(matches!(
        reopened.insert(&row(2, "blocked")),
        Err(StorageError::ChangeStream(ChangeStreamError::Unavailable(
            _
        )))
    ));
    let view = reopened.read_view().expect("read view after failure");
    assert_eq!(
        reopened
            .scan_columns_with_view(&[ColumnId(1), ColumnId(2)], &view)
            .expect("read after failed commit")
            .into_iter()
            .map(|(_, values)| values)
            .collect::<Vec<_>>(),
        vec![row(1, "before")]
    );
    drop(view);
    reopened
        .disable_change_stream()
        .expect("explicitly abandon");
    reopened
        .insert(&row(2, "allowed"))
        .expect("write after disable");
    reopened.close().expect("close");
    cleanup_heap(&path);
}

#[test]
fn missing_active_log_is_unavailable_until_explicit_rebaseline() {
    let path = path("missing-active").with_extension("db");
    cleanup_heap(&path);
    let mut storage = TableStorage::create_heap_with_storage_id(&path, table(), StorageId(44))
        .expect("create heap");
    let old = storage.enable_change_stream().expect("enable");
    storage.close().expect("close");
    std::fs::remove_file(heap_change_log_path(&path)).expect("remove active log");

    let mut reopened = TableStorage::open_heap(&path, table()).expect("open authoritative heap");
    assert_eq!(
        reopened.inspect_change_stream().status,
        netbadb_storage::ChangeStreamStatus::Unavailable
    );
    assert!(matches!(
        reopened.insert(&row(1, "blocked")),
        Err(StorageError::ChangeStream(ChangeStreamError::Unavailable(
            _
        )))
    ));
    reopened
        .disable_change_stream()
        .expect("abandon missing log");
    let fresh = reopened.enable_change_stream().expect("rebaseline");
    assert_ne!(old.generation, fresh.generation);
    assert!(matches!(
        reopened.read_changes(old, 1, 1_000),
        Err(StorageError::ChangeStream(
            ChangeStreamError::StreamIdentityMismatch
        ))
    ));
    reopened.close().expect("close reopened");
    cleanup_heap(&path);
}
