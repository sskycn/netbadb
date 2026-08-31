#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| {
    netbadb_core::fuzz_schema_catalog_bytes(bytes);
});
