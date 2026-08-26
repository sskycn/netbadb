#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT_SIZE: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_SIZE {
        netbadb_core::fuzz_partition_catalog_bytes(data);
    }
});
