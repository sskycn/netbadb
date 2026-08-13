#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_index::{IndexSpec, decode_internal, decode_leaf, decode_meta};
use netbadb_types::{PhysicalType, SemanticType};

const MAX_PAYLOAD_SIZE: usize = 4_060;

fuzz_target!(|data: &[u8]| {
    let (kind, payload) = data
        .split_first()
        .map_or((0, data), |(&kind, payload)| (kind, payload));
    if payload.len() > MAX_PAYLOAD_SIZE {
        return;
    }
    let spec = IndexSpec {
        data_type: SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    match kind % 3 {
        0 => {
            let _ = decode_meta(payload);
        }
        1 => {
            let _ = decode_leaf(&spec, payload);
        }
        _ => {
            let _ = decode_internal(&spec, payload);
        }
    }
});
