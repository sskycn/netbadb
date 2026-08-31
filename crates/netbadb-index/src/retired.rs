//! Independent NBTR v1 payload. The enclosing Page v5 retains its leaf/internal
//! kind and CRC; that kind does not confer active-node semantics on this payload.
use crate::{Decoder, IndexError, validate_generation};
use netbadb_types::{IndexId, PageGeneration, PageId, PageRef};

const MAGIC: &[u8; 4] = b"NBTR";
pub const RETIRED_BTREE_FORMAT_VERSION: u16 = 1;

/// A terminal state within one allocation. No child, leaf-link, or root fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredBTreePage {
    pub page_ref: PageRef,
    pub owner: IndexId,
}

/// 32 bytes: NBTR, u16 version=1, u16 reserved=0, u64 owner,
/// u64 generation, u64 physical PageId. Integers are little endian.
pub fn encode_retired_btree(page: RetiredBTreePage) -> Result<Vec<u8>, IndexError> {
    validate_generation(page.page_ref.generation)?;
    if page.owner.0 == 0 {
        return Err(IndexError::InvalidIndexId(page.owner));
    }
    if page.page_ref.page_id.0 == 0 {
        return Err(IndexError::InvalidChild(page.page_ref.page_id));
    }
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&RETIRED_BTREE_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&page.owner.0.to_le_bytes());
    bytes.extend_from_slice(&page.page_ref.generation.0.to_le_bytes());
    bytes.extend_from_slice(&page.page_ref.page_id.0.to_le_bytes());
    Ok(bytes)
}

pub fn decode_retired_btree(bytes: &[u8]) -> Result<RetiredBTreePage, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let actual = decoder.array::<4>()?;
    if actual != *MAGIC {
        return Err(IndexError::InvalidMagic {
            expected: *MAGIC,
            actual,
        });
    }
    let version = decoder.u16()?;
    if version != RETIRED_BTREE_FORMAT_VERSION {
        return Err(IndexError::UnsupportedVersion(version));
    }
    if decoder.u16()? != 0 {
        return Err(IndexError::InvalidReservedBytes);
    }
    let owner = IndexId(decoder.u64()?);
    let generation = PageGeneration(decoder.u64()?);
    let page_id = PageId(decoder.u64()?);
    let page = RetiredBTreePage {
        owner,
        page_ref: PageRef {
            page_id,
            generation,
        },
    };
    // Reuse the encoder's identity validation and reject every trailing byte.
    if encode_retired_btree(page)?.as_slice() != bytes {
        return Err(IndexError::InvalidNodeType);
    }
    Ok(page)
}

/// Detects the marker magic, then fully validates it. A malformed marker is an
/// error, never an active node or an absent retirement proof.
pub fn retired_btree_page(bytes: &[u8]) -> Result<Option<RetiredBTreePage>, IndexError> {
    if bytes.starts_with(MAGIC) {
        decode_retired_btree(bytes).map(Some)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn marker_is_strict_and_never_an_active_node() {
        let marker = RetiredBTreePage {
            owner: IndexId(9),
            page_ref: PageRef {
                page_id: PageId(12),
                generation: PageGeneration(100),
            },
        };
        let bytes = encode_retired_btree(marker).unwrap();
        assert_eq!(decode_retired_btree(&bytes).unwrap(), marker);
        for length in 0..bytes.len() {
            assert!(decode_retired_btree(&bytes[..length]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode_retired_btree(&extra).is_err());
        for offset in [8, 16, 24] {
            let mut bad = bytes.clone();
            bad[offset..offset + 8].fill(0);
            assert!(decode_retired_btree(&bad).is_err());
        }
        for offset in [0, 4, 6] {
            let mut bad = bytes.clone();
            bad[offset] ^= 0x80;
            assert!(decode_retired_btree(&bad).is_err());
        }
        let spec = crate::IndexSpec {
            data_type: netbadb_types::SemanticType::physical(netbadb_types::PhysicalType::UInt64),
            nullable: true,
        };
        assert!(crate::decode_leaf_owned(&spec, &bytes, Some(marker.owner)).is_err());
        assert!(crate::decode_internal_owned(&spec, &bytes, Some(marker.owner)).is_err());
        assert!(crate::decode_meta(&bytes).is_err());
        assert_eq!(crate::btree_page_owner(&bytes).unwrap(), Some(marker.owner));
        assert_eq!(
            crate::btree_page_generation(&bytes).unwrap(),
            Some(marker.page_ref.generation)
        );
    }
}
