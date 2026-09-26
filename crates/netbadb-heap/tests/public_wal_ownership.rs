use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use netbadb_heap::{
    HeapOwnership, HeapStorage, HeapStorageError, PageManager, WalError, WalManager, WalRecordKind,
    wal_alternate_path, wal_path,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "rows",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}

fn temp_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-public-wal-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn wait_with_timeout(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("ownership child timed out");
        }
        std::thread::yield_now();
    }
}

fn directory_snapshot(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            let bytes = if entry.file_type().unwrap().is_file() {
                std::fs::read(entry.path()).unwrap()
            } else {
                Vec::new()
            };
            (name, bytes)
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

#[cfg(all(unix, feature = "test-hooks"))]
fn file_identity(path: &Path) -> Option<(u64, u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    match path.symlink_metadata() {
        Ok(metadata) => Some((metadata.dev(), metadata.ino(), metadata.len())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("cannot inspect {}: {error}", path.display()),
    }
}

#[cfg(unix)]
#[test]
fn active_alternate_and_reserved_slot_reject_public_writers() {
    if let Some(path) = std::env::var_os("NETBADB_R02_OPEN") {
        let error = WalManager::open(PathBuf::from(path)).unwrap_err();
        assert!(
            matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
            "{error:?}"
        );
        return;
    }
    if let Some(path) = std::env::var_os("NETBADB_R02_CREATE") {
        let error = WalManager::create(PathBuf::from(path)).unwrap_err();
        assert!(
            matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
            "{error:?}"
        );
        return;
    }

    let dir = temp_dir("r02-generation-slot");
    let heap = dir.join("data.heap");
    let mut storage = HeapStorage::create(&heap, table()).unwrap();
    let root = wal_path(&heap);
    let next = wal_alternate_path(&root);
    assert!(!next.exists());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "active_alternate_and_reserved_slot_reject_public_writers",
            "--nocapture",
        ])
        .env("NETBADB_R02_CREATE", &next)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success());
    assert!(
        !next.exists(),
        "rejected create must not initialize the target WAL"
    );
    storage.checkpoint().unwrap();
    assert!(next.exists());
    let before = directory_snapshot(&dir);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "active_alternate_and_reserved_slot_reject_public_writers",
            "--nocapture",
        ])
        .env("NETBADB_R02_OPEN", &next)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success());
    assert_eq!(directory_snapshot(&dir), before);
    assert!(
        !root.exists(),
        "the first checkpoint retired the root generation"
    );
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "active_alternate_and_reserved_slot_reject_public_writers",
            "--nocapture",
        ])
        .env("NETBADB_R02_CREATE", &root)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success());
    assert!(!root.exists());
    storage.checkpoint().unwrap();
    assert!(root.exists());
    assert!(!next.exists());
    storage.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn active_alternate_public_open_cannot_preempt_heap_recovery() {
    if let Some(path) = std::env::var_os("NETBADB_R02_ACTIVE_ALTERNATE") {
        let error = WalManager::open(PathBuf::from(path)).unwrap_err();
        assert!(
            matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
            "{error:?}"
        );
        return;
    }
    let dir = temp_dir("r02-active-alternate");
    let heap = dir.join("data.heap");
    let mut storage = HeapStorage::create(&heap, table()).unwrap();
    storage.checkpoint().unwrap();
    let alternate = wal_alternate_path(wal_path(&heap));
    assert!(alternate.exists());
    let before = std::fs::read(&alternate).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "active_alternate_public_open_cannot_preempt_heap_recovery",
            "--nocapture",
        ])
        .env("NETBADB_R02_ACTIVE_ALTERNATE", &alternate)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success());
    assert_eq!(std::fs::read(&alternate).unwrap(), before);
    storage.close().unwrap();
    let raw = WalManager::open(&alternate).unwrap();
    let error = HeapStorage::open(&heap, table()).unwrap_err();
    assert!(
        matches!(error, HeapStorageError::Wal(WalError::Io(ref source)) if source.kind() == std::io::ErrorKind::WouldBlock),
        "{error:?}"
    );
    raw.close().unwrap();
    HeapStorage::open(&heap, table()).unwrap().close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn independent_next_name_and_disjoint_wals_remain_usable() {
    use std::os::unix::fs::symlink;

    let dir = temp_dir("r02-independent-next");
    let named_next = dir.join("independent.next");
    let other = dir.join("unrelated.wal");
    let first = WalManager::create(&named_next).unwrap();
    let second = WalManager::create(&other).unwrap();
    let parent_alias = dir.with_extension("directory-alias");
    symlink(&dir, &parent_alias).unwrap();
    let alias = parent_alias.join("independent.next");
    let error = WalManager::open(&alias).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
        "{error:?}"
    );
    assert_eq!(first.path(), named_next.canonicalize().unwrap());
    assert!(named_next.exists());
    assert!(other.exists());
    first.close().unwrap();
    second.close().unwrap();
    WalManager::open(&named_next).unwrap().close().unwrap();
    std::fs::remove_file(parent_alias).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(all(unix, feature = "test-hooks"))]
#[test]
fn rotation_slot_is_exclusive_at_each_mutation_window() {
    use netbadb_heap::{WalRotationTestPoint, set_wal_rotation_test_hook};
    use std::sync::mpsc;

    if let Some(path) = std::env::var_os("NETBADB_R02_ROTATION_PROBE") {
        let path = PathBuf::from(path);
        let result = if std::env::var_os("NETBADB_R02_ROTATION_CREATE").is_some() {
            WalManager::create(&path)
        } else {
            WalManager::open(&path)
        };
        let error = result.unwrap_err();
        assert!(
            matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
            "{error:?}"
        );
        return;
    }

    let dir = temp_dir("r02-rotation-windows");
    let heap = dir.join("data.heap");
    let root = wal_path(&heap);
    let next = wal_alternate_path(&root);
    // The standalone probe claims its own second carrier before it reaches
    // the shared slot. Keep lock-infrastructure creation outside snapshots.
    std::fs::write(netbadb_heap::wal_owner_path(wal_alternate_path(&next)), []).unwrap();
    let (phase_send, phase_recv) = mpsc::channel();
    let (resume_send, resume_recv) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut storage = HeapStorage::create(&heap, table()).unwrap();
        set_wal_rotation_test_hook(move |point| {
            phase_send.send(point).unwrap();
            resume_recv.recv().unwrap();
        });
        storage.checkpoint().unwrap();
        storage.close().unwrap();
    });
    for expected in [
        WalRotationTestPoint::BeforeTargetCreation,
        WalRotationTestPoint::AfterTargetCreated,
        WalRotationTestPoint::AfterTargetDirectorySync,
        WalRotationTestPoint::AfterRuntimeSwitch,
        WalRotationTestPoint::AfterOldGenerationRemoved,
    ] {
        let observed = phase_recv.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(observed, expected);
        let (path, create) = match observed {
            WalRotationTestPoint::BeforeTargetCreation => (&next, true),
            WalRotationTestPoint::AfterTargetCreated
            | WalRotationTestPoint::AfterTargetDirectorySync => (&next, false),
            WalRotationTestPoint::AfterRuntimeSwitch => (&root, false),
            WalRotationTestPoint::AfterOldGenerationRemoved => (&root, true),
        };
        assert_eq!(path.exists(), !create);
        let before = directory_snapshot(&dir);
        let root_identity = file_identity(&root);
        let next_identity = file_identity(&next);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "rotation_slot_is_exclusive_at_each_mutation_window",
                "--nocapture",
            ])
            .env("NETBADB_R02_ROTATION_PROBE", path);
        if create {
            command.env("NETBADB_R02_ROTATION_CREATE", "1");
        }
        let mut child = command.spawn().unwrap();
        assert!(
            wait_with_timeout(&mut child).success(),
            "rotation probe at {observed:?}"
        );
        assert_eq!(directory_snapshot(&dir), before);
        assert_eq!(file_identity(&root), root_identity);
        assert_eq!(file_identity(&next), next_identity);
        resume_send.send(()).unwrap();
    }
    worker.join().unwrap();
    assert!(next.exists());
    assert!(!root.exists());
    HeapStorage::open(dir.join("data.heap"), table())
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn wal_data_aliases_cannot_become_writers() {
    use std::os::unix::fs::{MetadataExt, symlink};

    let dir = temp_dir("r02-data-alias");
    let root = dir.join("standalone.wal");
    let writer = WalManager::create(&root).unwrap();
    let link = dir.join("symlink.wal");
    symlink(&root, &link).unwrap();
    assert_eq!(
        std::fs::metadata(&root).unwrap().ino(),
        std::fs::metadata(&link).unwrap().ino()
    );
    let error = WalManager::open(&link).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::InvalidInput),
        "{error:?}"
    );
    let hard = dir.join("hardlink.wal");
    std::fs::hard_link(&root, &hard).unwrap();
    assert_eq!(
        std::fs::metadata(&root).unwrap().ino(),
        std::fs::metadata(&hard).unwrap().ino()
    );
    let error = WalManager::open(&hard).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::InvalidInput),
        "{error:?}"
    );
    drop(writer);
    std::fs::remove_file(&link).unwrap();
    std::fs::remove_file(&hard).unwrap();
    WalManager::open(&root).unwrap().close().unwrap();

    let heap = dir.join("data.heap");
    let mut storage = HeapStorage::create(&heap, table()).unwrap();
    storage.checkpoint().unwrap();
    let alternate = wal_alternate_path(wal_path(&heap));
    let alternate_link = dir.join("active-alternate-link.wal");
    symlink(&alternate, &alternate_link).unwrap();
    assert_eq!(
        std::fs::metadata(&alternate).unwrap().ino(),
        std::fs::metadata(&alternate_link).unwrap().ino()
    );
    let error = WalManager::open(&alternate_link).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::InvalidInput),
        "{error:?}"
    );
    std::fs::remove_file(&alternate_link).unwrap();
    let alternate_hard = dir.join("active-alternate-hard.wal");
    std::fs::hard_link(&alternate, &alternate_hard).unwrap();
    let error = WalManager::open(&alternate_hard).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::InvalidInput),
        "{error:?}"
    );
    std::fs::remove_file(&alternate_hard).unwrap();
    storage.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn wal_data_hardlink_alone_cannot_claim_another_carrier() {
    use std::os::unix::fs::MetadataExt;

    let dir = temp_dir("r02-hardlink-alone");
    let root = dir.join("source.wal");
    let alias = dir.join("alias.wal");
    let writer = WalManager::create(&root).unwrap();
    std::fs::hard_link(&root, &alias).unwrap();
    assert_eq!(
        std::fs::metadata(&root).unwrap().ino(),
        std::fs::metadata(&alias).unwrap().ino()
    );
    let original = std::fs::read(&root).unwrap();
    let error = WalManager::open(&alias).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::InvalidInput),
        "{error:?}"
    );
    assert_eq!(std::fs::read(&root).unwrap(), original);
    assert_eq!(std::fs::read(&alias).unwrap(), original);
    std::fs::remove_file(alias).unwrap();
    writer.close().unwrap();
    WalManager::open(&root).unwrap().close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn failed_incremental_admission_releases_only_new_carriers() {
    let dir = temp_dir("r02-failed-incremental");
    let heap = dir.join("data.heap");
    HeapStorage::create(&heap, table())
        .unwrap()
        .close()
        .unwrap();
    let unrelated = dir.join("middle.wal");
    let contested = dir.join("z-contested.wal");
    let temporary = dir.join("a-temporary.wal");
    let mut owner = HeapOwnership::acquire_with_wal(&heap, &unrelated).unwrap();
    let raw = WalManager::create(&contested).unwrap();
    let error = owner
        .add_wal_owner_binding(&contested, netbadb_heap::wal_owner_path(&temporary))
        .unwrap_err();
    assert!(
        matches!(error, HeapStorageError::Wal(WalError::Io(ref source)) if source.kind() == std::io::ErrorKind::WouldBlock),
        "{error:?}"
    );
    // The first, temporary carrier was acquired before contention and must
    // no longer be locked. The original Heap admission is still retained.
    WalManager::create(&temporary).unwrap().close().unwrap();
    let error = WalManager::create(&unrelated).unwrap_err();
    assert!(
        matches!(error, WalError::Io(ref source) if source.kind() == std::io::ErrorKind::WouldBlock),
        "{error:?}"
    );
    drop(owner);
    WalManager::create(&unrelated).unwrap().close().unwrap();
    raw.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn public_wal_entry_rejects_heap_owner_across_processes() {
    if let Some(path) = std::env::var_os("NETBADB_STANDALONE_WAL_CHILD_PATH") {
        let path = PathBuf::from(path);
        eprintln!("child attempting standalone WAL open: {}", path.display());
        assert!(WalManager::open(&path).is_err());
        return;
    }
    if let Some(path) = std::env::var_os("NETBADB_WAL_CHILD_PATH") {
        let path = PathBuf::from(path);
        eprintln!("child attempting public WAL open: {}", path.display());
        assert!(WalManager::open(&path).is_err());
        assert!(PageManager::open(path.with_extension("heap")).is_err());
        return;
    }

    let dir = temp_dir("heap-blocks-raw");
    let heap_path = dir.join("data.heap");
    let mut storage = HeapStorage::create(&heap_path, table()).unwrap();
    storage.checkpoint().unwrap();
    let wal = wal_path(&heap_path);
    let before = directory_snapshot(&dir);
    let before_heap = std::fs::read(&heap_path).unwrap();

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("public_wal_entry_rejects_heap_owner_across_processes")
        .arg("--nocapture")
        .env("NETBADB_WAL_CHILD_PATH", &wal)
        .spawn()
        .unwrap();
    let status = wait_with_timeout(&mut child);
    assert!(status.success(), "ownership child failed: {status}");
    assert_eq!(std::fs::read(&heap_path).unwrap(), before_heap);
    assert_eq!(directory_snapshot(&dir), before);

    storage.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn public_standalone_wal_writers_are_exclusive_and_release_on_drop() {
    let dir = temp_dir("standalone");
    let wal_path = dir.join("independent.log");
    let writer = WalManager::create(&wal_path).unwrap();
    assert!(WalManager::open(&wal_path).is_err());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("public_wal_entry_rejects_heap_owner_across_processes")
        .arg("--nocapture")
        .env("NETBADB_STANDALONE_WAL_CHILD_PATH", &wal_path)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success());
    drop(writer);
    let reopened = WalManager::open(&wal_path).unwrap();
    reopened.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn standalone_public_wal_writer_blocks_heap_recovery_without_mutation() {
    let dir = temp_dir("raw-blocks-heap");
    let heap_path = dir.join("data.heap");
    HeapStorage::create(&heap_path, table())
        .unwrap()
        .close()
        .unwrap();
    let wal = wal_path(&heap_path);
    let writer = WalManager::open(&wal).unwrap();
    let before = directory_snapshot(&dir);
    let before_heap = std::fs::read(&heap_path).unwrap();
    let before_wal = std::fs::read(&wal).unwrap();

    assert!(HeapStorage::open(&heap_path, table()).is_err());
    assert_eq!(std::fs::read(&heap_path).unwrap(), before_heap);
    assert_eq!(std::fs::read(&wal).unwrap(), before_wal);
    assert_eq!(directory_snapshot(&dir), before);

    writer.close().unwrap();
    HeapStorage::open(&heap_path, table())
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn claim_with_unrelated_carrier(
    heap: &Path,
    carrier: &Path,
    entry: &str,
) -> Result<HeapOwnership, HeapStorageError> {
    let wal = wal_path(heap);
    match entry {
        "single" => HeapOwnership::acquire_with_wal_owner(heap, wal, carrier),
        "batch" => HeapOwnership::acquire_with_wal_bindings(heap, &[(wal, carrier.to_path_buf())]),
        "incremental" => {
            // First hold the Heap with a different logical root. The actual
            // WAL root is introduced only through the incremental API.
            let mut owner =
                HeapOwnership::acquire_with_wal(heap, carrier.with_extension("unrelated-wal"))?;
            owner.add_wal_owner_binding(&wal, carrier)?;
            Ok(owner)
        }
        _ => panic!("unknown ownership entry"),
    }
}

#[cfg(unix)]
fn assert_unrelated_carrier_excluded(entry: &str, test_name: &str) {
    if let Some(heap) = std::env::var_os("NETBADB_UNRELATED_CARRIER_HEAP") {
        let heap = PathBuf::from(heap);
        let carrier = PathBuf::from(std::env::var_os("NETBADB_UNRELATED_CARRIER").unwrap());
        let error = match claim_with_unrelated_carrier(&heap, &carrier, entry) {
            Ok(owner) => match HeapStorage::open_with_ownership(owner, table(), None) {
                Ok(_) => panic!("second writable Heap opened through {entry}"),
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(
            matches!(error, HeapStorageError::Wal(WalError::Io(ref source))
                if source.kind() == std::io::ErrorKind::WouldBlock),
            "{entry}: expected contention on the real WAL carrier, got {error:?}"
        );
        return;
    }

    let dir = temp_dir(entry);
    let heap = dir.join("data.heap");
    HeapStorage::create(&heap, table())
        .unwrap()
        .close()
        .unwrap();
    let wal = wal_path(&heap);
    let carrier = dir.join("unrelated.owner-lock");
    std::fs::write(&carrier, []).unwrap();
    if entry == "incremental" {
        let unrelated = carrier.with_extension("unrelated-wal");
        for root in [&unrelated, &wal_alternate_path(&unrelated)] {
            std::fs::write(netbadb_heap::wal_owner_path(root), []).unwrap();
        }
    }
    let mut writer = WalManager::open(&wal).unwrap();
    let before = directory_snapshot(&dir);

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env("NETBADB_UNRELATED_CARRIER_HEAP", &heap)
        .env("NETBADB_UNRELATED_CARRIER", &carrier)
        .spawn()
        .unwrap();
    assert!(wait_with_timeout(&mut child).success(), "entry {entry}");
    assert_eq!(directory_snapshot(&dir), before, "entry {entry}");
    let txn = writer.next_txn_id();
    let begin = writer.append(txn, None, WalRecordKind::Begin).unwrap();
    let abort = writer
        .append(txn, Some(begin), WalRecordKind::Abort)
        .unwrap();
    let complete = writer
        .append(txn, Some(abort), WalRecordKind::RollbackComplete)
        .unwrap();
    writer.flush_through(complete).unwrap();
    writer.close().unwrap();
    HeapStorage::open(&heap, table()).unwrap().close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn unrelated_carrier_single_binding_rejects_raw_writer() {
    assert_unrelated_carrier_excluded(
        "single",
        "unrelated_carrier_single_binding_rejects_raw_writer",
    );
}

#[cfg(unix)]
#[test]
fn unrelated_carrier_batch_binding_rejects_raw_writer() {
    assert_unrelated_carrier_excluded(
        "batch",
        "unrelated_carrier_batch_binding_rejects_raw_writer",
    );
}

#[cfg(unix)]
#[test]
fn unrelated_carrier_incremental_binding_rejects_raw_writer() {
    assert_unrelated_carrier_excluded(
        "incremental",
        "unrelated_carrier_incremental_binding_rejects_raw_writer",
    );
}

#[cfg(unix)]
#[test]
fn unrelated_carrier_without_competitor_uses_real_wal_protection() {
    let dir = temp_dir("uncontended-unrelated");
    let heap = dir.join("data.heap");
    HeapStorage::create(&heap, table())
        .unwrap()
        .close()
        .unwrap();
    let carrier = dir.join("unrelated.owner-lock");
    let owner = claim_with_unrelated_carrier(&heap, &carrier, "single").unwrap();
    let storage = HeapStorage::open_with_ownership(owner, table(), None).unwrap();
    let error = WalManager::open(wal_path(&heap)).unwrap_err();
    assert!(
        matches!(error, WalError::Io(source) if source.kind() == std::io::ErrorKind::WouldBlock)
    );
    storage.close().unwrap();
    WalManager::open(wal_path(&heap)).unwrap().close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn same_inode_carrier_alias_reopens_without_self_contention() {
    let dir = temp_dir("carrier-alias");
    let heap = dir.join("data.heap");
    HeapStorage::create(&heap, table())
        .unwrap()
        .close()
        .unwrap();
    let wal = wal_path(&heap);
    let alias = dir.join("staged.owner-lock");
    std::fs::hard_link(netbadb_heap::wal_owner_path(&wal), &alias).unwrap();

    let owner = HeapOwnership::acquire_with_wal_owner(&heap, &wal, &alias).unwrap();
    let storage = HeapStorage::open_with_ownership(owner, table(), None).unwrap();
    assert!(matches!(
        WalManager::open(&wal),
        Err(WalError::Io(source)) if source.kind() == std::io::ErrorKind::WouldBlock
    ));
    storage.close().unwrap();
    WalManager::open(&wal).unwrap().close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
