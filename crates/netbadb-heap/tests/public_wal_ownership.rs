use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use netbadb_heap::{HeapStorage, PageManager, WalManager, wal_path};
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
