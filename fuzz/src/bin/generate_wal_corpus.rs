use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, WalManager, WalRecordKind, wal_path};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId, TxnId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os().nth(1).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus/wal_recovery"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("empty"), [])?;

    write_seed(&output, "valid-header", |_, _| Ok(()))?;
    write_seed(&output, "valid-begin", |wal, _| {
        wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        Ok(())
    })?;
    write_seed(&output, "valid-begin-commit", |wal, _| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        wal.append(TxnId(1), Some(begin), WalRecordKind::Commit)?;
        Ok(())
    })?;
    write_page_update_seed(&output)?;
    write_seed(&output, "truncated-final-record", |wal, path| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        let commit = wal.append(TxnId(1), Some(begin), WalRecordKind::Commit)?;
        wal.flush_through(commit)?;
        let truncated_len = std::fs::metadata(path)?.len() - 20;
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(truncated_len)?;
        Ok(())
    })?;
    Ok(())
}

fn write_page_update_seed(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let database_path =
        std::env::temp_dir().join(format!("netbadb-wal-corpus-{}-heap", std::process::id()));
    let wal_file = wal_path(&database_path);
    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(&wal_file);
    let mut storage = HeapStorage::create(&database_path, fuzz_table())?;
    storage.insert(&[ScalarValue::UInt64(1)])?;
    drop(storage);
    std::fs::copy(&wal_file, output.join("valid-page-update"))?;
    std::fs::remove_file(wal_file)?;
    std::fs::remove_file(database_path)?;
    Ok(())
}

fn fuzz_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "fuzz_rows",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::UInt64))
                .primary_key(true),
        ],
    )
}

fn write_seed(
    output: &Path,
    name: &str,
    build: impl FnOnce(&mut WalManager, &Path) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let path =
        std::env::temp_dir().join(format!("netbadb-wal-corpus-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut wal = WalManager::create(&path)?;
    build(&mut wal, &path)?;
    drop(wal);
    std::fs::copy(&path, output.join(name))?;
    std::fs::remove_file(path)?;
    Ok(())
}
