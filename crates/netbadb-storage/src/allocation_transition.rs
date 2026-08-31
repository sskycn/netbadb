//! The sole nonzero allocation boundary. Ordinary updates never cross it.
use netbadb_index::{
    IndexError, IndexSpec, btree_page_owner, decode_internal_owned, decode_leaf_owned, decode_meta,
};
use netbadb_types::{IndexId, Lsn, PageRef, PhysicalType, SemanticType};

use crate::{Page, PageType, StorageError};

/// Fully decode a self-identifying registered v3 page, without following dormant
/// outgoing edges. The payload's physical keys are self-describing; nominal
/// schema identity is not allocation authority.
pub(crate) fn identity(page: &Page) -> Result<(PageRef, IndexId), StorageError> {
    let kind = page.validated()?.header().page_type;
    if !matches!(
        kind,
        PageType::BTreeMeta | PageType::BTreeLeaf | PageType::BTreeInternal
    ) {
        return Err(IndexError::InvalidNodeType.into());
    }
    let payload = page.single_payload(kind)?;
    let owner = btree_page_owner(payload)?.ok_or(IndexError::InvalidNodeType)?;
    let generation = page
        .allocation_generation()?
        .ok_or(IndexError::InvalidNodeType)?;
    let reference = PageRef {
        page_id: page.id,
        generation,
    };
    if kind == PageType::BTreeMeta {
        decode_meta(payload)?;
        return Ok((reference, owner));
    }
    for physical in [
        PhysicalType::Bool,
        PhysicalType::Int64,
        PhysicalType::UInt64,
        PhysicalType::Text,
    ] {
        let spec = IndexSpec {
            data_type: SemanticType::physical(physical),
            nullable: true,
        };
        let valid = match kind {
            PageType::BTreeLeaf => decode_leaf_owned(&spec, payload, Some(owner)).is_ok(),
            PageType::BTreeInternal => decode_internal_owned(&spec, payload, Some(owner)).is_ok(),
            _ => false,
        };
        if valid {
            return Ok((reference, owner));
        }
    }
    Err(IndexError::InvalidNodeType.into())
}

pub(crate) fn validate(before: &Page, after: &Page) -> Result<(), StorageError> {
    let (old, old_owner) = identity(before)?;
    let (new, new_owner) = identity(after)?;
    if old.page_id != new.page_id || old.generation >= new.generation || old_owner == new_owner {
        return Err(crate::invalid_format(
            "invalid BTree allocation transition identities",
        ));
    }
    Ok(())
}

/// Returns whether redo must install the after image. An old incarnation never
/// uses its LSN to skip the boundary. New incarnation LSNs cannot precede it.
pub(crate) fn redo(
    current: &Page,
    before: &Page,
    after: &Page,
    lsn: Lsn,
) -> Result<bool, StorageError> {
    validate(before, after)?;
    let actual = identity(current)?;
    if actual == identity(before)? {
        // A retired allocation cannot change between claim and publication.
        if current.bytes() != before.bytes() {
            return Err(crate::invalid_format(
                "transition old allocation differs from before image",
            ));
        }
        return Ok(true);
    }
    if actual != identity(after)? {
        return Err(crate::invalid_format(
            "transition encountered unrelated allocation",
        ));
    }
    let current_lsn = current.page_lsn()?;
    if current_lsn < Some(lsn) || (current_lsn == Some(lsn) && current.bytes() != after.bytes()) {
        return Err(crate::invalid_format(
            "transition new allocation has invalid pageLSN or image",
        ));
    }
    Ok(false)
}

/// Returns whether undo must restore the byte-exact before image. The caller
/// reverses subsequent same-allocation updates before crossing this boundary.
pub(crate) fn undo(current: &Page, before: &Page, after: &Page) -> Result<bool, StorageError> {
    validate(before, after)?;
    let actual = identity(current)?;
    if actual == identity(before)? {
        if current.bytes() != before.bytes() {
            return Err(crate::invalid_format(
                "undone transition differs from before image",
            ));
        }
        return Ok(false);
    }
    if actual != identity(after)? {
        return Err(crate::invalid_format(
            "transition undo encountered unrelated allocation",
        ));
    }
    Ok(true)
}

#[cfg(test)]
#[path = "allocation_transition_tests.rs"]
mod tests;
