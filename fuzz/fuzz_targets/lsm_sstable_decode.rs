#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT_SIZE: usize = 128 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_SIZE {
        netbadb_storage::fuzz_lsm_sstable_block_bytes(data);
    }
});
