#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_index::{decode_index_catalog, encode_index_catalog};

const MAX_PAYLOAD_SIZE: usize = 4_060;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_PAYLOAD_SIZE {
        if let Ok(node) = decode_index_catalog(data) {
            let encoded = encode_index_catalog(&node).expect("decoded catalog must re-encode");
            assert_eq!(
                decode_index_catalog(&encoded).expect("canonical catalog"),
                node
            );
        }
    }
});
