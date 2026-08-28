#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_pgwire::{MAX_MESSAGE_BYTES, read_frontend_message, read_startup_packet};

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_MESSAGE_BYTES + 5 {
        return;
    }
    if data.first().is_some_and(|selector| selector & 1 == 0) {
        let _ = read_startup_packet(&mut data.get(1..).unwrap_or_default());
    } else {
        let _ = read_frontend_message(&mut data.get(1..).unwrap_or_default());
    }
});
