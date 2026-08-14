#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_protocol::{decode_client_frame, decode_server_frame};

const MAX_INPUT_SIZE: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() <= MAX_INPUT_SIZE {
        let _ = decode_client_frame(data);
        let _ = decode_server_frame(data);
    }
});
