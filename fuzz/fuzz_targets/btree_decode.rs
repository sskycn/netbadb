#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_index::{
    IndexSpec, btree_page_owner, decode_internal_owned, decode_leaf_owned, decode_meta,
    encode_internal_owned, encode_leaf_owned, encode_meta,
};
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
    let Ok(owner) = btree_page_owner(payload) else {
        return;
    };
    match kind % 3 {
        0 => {
            if let Ok(node) = decode_meta(payload) {
                assert_eq!(decode_meta(&encode_meta(&node).unwrap()).unwrap(), node);
            }
        }
        1 => {
            if let Ok(node) = decode_leaf_owned(&spec, payload, owner) {
                assert_eq!(
                    decode_leaf_owned(
                        &spec,
                        &encode_leaf_owned(&spec, &node, owner).unwrap(),
                        owner
                    )
                    .unwrap(),
                    node
                );
                assert!(
                    decode_leaf_owned(
                        &spec,
                        payload,
                        if owner.is_some() {
                            None
                        } else {
                            Some(netbadb_types::IndexId(1))
                        }
                    )
                    .is_err()
                );
            }
        }
        _ => {
            if let Ok(node) = decode_internal_owned(&spec, payload, owner) {
                assert_eq!(
                    decode_internal_owned(
                        &spec,
                        &encode_internal_owned(&spec, &node, owner).unwrap(),
                        owner
                    )
                    .unwrap(),
                    node
                );
            }
        }
    }
});
