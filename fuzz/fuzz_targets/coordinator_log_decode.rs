#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT_SIZE: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }
    let path =
        std::env::temp_dir().join(format!("netbadb-coordinator-fuzz-{}", std::process::id()));
    if std::fs::write(&path, data).is_ok() {
        netbadb_core::fuzz_coordinator_log_file(&path);
    }
    let _ = std::fs::remove_file(path);
});
