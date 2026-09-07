use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Instant;

use netbadb_core::{Database, DatabaseCoordinatorConfig, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "items",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}

fn root(transactions: u64) -> PathBuf {
    std::env::temp_dir().join(format!(
        "netbadb-phase3b5-compaction-{transactions}-{}",
        std::process::id()
    ))
}

fn open_timed(catalog: &Path) -> Result<(Database, u128), Box<dyn Error>> {
    let started = Instant::now();
    let database = Database::open_catalog(catalog)?;
    Ok((database, started.elapsed().as_nanos()))
}

fn run(transactions: u64) -> Result<(), Box<dyn Error>> {
    let root = root(transactions);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let catalog = root.join("catalog");
    let coordinator = root.join("coordinator.nbco");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(
            root.join("items.heap"),
            table(),
        )],
        Some(DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility()),
    )?;
    for id in 1..=transactions {
        database.execute(&format!("INSERT INTO items VALUES ({id})"))?;
    }
    database.close()?;

    let (mut database, open_before_ns) = open_timed(&catalog)?;
    let before = database.inspect_global_visibility()?;
    let compaction_started = Instant::now();
    let report = database.compact_coordinator_log()?;
    let compaction_ns = compaction_started.elapsed().as_nanos();
    database.close()?;

    let (mut database, open_after_ns) = open_timed(&catalog)?;
    let after = database.inspect_global_visibility()?;
    let next_transaction = database.begin_transaction()?;
    let next_database_txn_id = next_transaction.id().0;
    drop(next_transaction);
    database.execute(&format!("INSERT INTO items VALUES ({})", transactions + 1))?;
    let continued = database.inspect_global_visibility()?;
    println!(
        "{transactions},{},{},{},{},{},{},{open_before_ns},{compaction_ns},{open_after_ns},{},{},{},{},{}",
        before.published_commit_seq.map_or(0, |sequence| sequence.0),
        report.bytes_before,
        report.bytes_after,
        report.bytes_reclaimed,
        before.retained_decision_count,
        after.retained_decision_count,
        after.next_commit_seq.map_or(0, |sequence| sequence.0),
        report.database_txn_id_high_water.0,
        next_database_txn_id,
        continued
            .published_commit_seq
            .map_or(0, |sequence| sequence.0),
        continued.next_commit_seq.map_or(0, |sequence| sequence.0),
    );
    database.close()?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn configured_counts() -> Result<Vec<u64>, Box<dyn Error>> {
    std::env::var("NETBADB_COORDINATOR_COMPACTION_TXNS")
        .unwrap_or_else(|_| "1000".to_owned())
        .split(',')
        .map(|part| {
            let count = part.trim().parse::<u64>()?;
            if count == 0 {
                return Err("transaction count must be nonzero".into());
            }
            Ok(count)
        })
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "transactions,published_g_before,bytes_before,bytes_after,bytes_reclaimed,retained_before,retained_after,open_before_ns,compaction_ns,open_after_ns,next_g_after_reopen,checkpoint_txn_high_water,next_database_txn_id,published_g_after_write,next_g_after_write"
    );
    for transactions in configured_counts()? {
        run(transactions)?;
    }
    Ok(())
}
