#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_storage::{PAGE_SIZE, Page};
use netbadb_types::{PageId, SlotId};

fuzz_target!(|data: &[u8]| {
    if data.len() > PAGE_SIZE {
        return;
    }

    let mut bytes = [0; PAGE_SIZE];
    bytes[..data.len()].copy_from_slice(data);
    let input_slot = data
        .get(..2)
        .map(|bytes| SlotId(u16::from_le_bytes([bytes[0], bytes[1]])))
        .unwrap_or(SlotId(0));

    // The corpus contains ordinary page seeds bound to page 7 and the
    // singleton index-catalog page bound to page 1. Page v5 checksums include
    // the page id, so try both identities without duplicating private format
    // tags in the fuzz harness.
    for page_id in [PageId(7), PageId(1)] {
        let page = Page::from_bytes(page_id, bytes);
        let Ok(header) = page.header() else {
            continue;
        };

        for slot in (0..header.slot_count).map(SlotId).chain([input_slot]) {
            let _ = page.slot(slot);
            let _ = page.slot_state(slot);
            let _ = page.is_slot_deleted(slot);
            let _ = page.read_record(slot);
        }
    }
});
