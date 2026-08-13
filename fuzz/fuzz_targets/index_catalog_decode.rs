#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_index::decode_index_catalog;

const MAX_PAYLOAD_SIZE: usize = 4_060;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_PAYLOAD_SIZE {
        let _ = decode_index_catalog(data);
    }
});
