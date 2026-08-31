#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| {
    if bytes.len() <= 16 * 1024 * 1024 {
        netbadb_core::fuzz_schema_mutation_bytes(bytes);
    }
});
