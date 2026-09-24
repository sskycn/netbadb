//! Shared test-only production-path inspection counters.

use std::cell::Cell;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Activity {
    pub scan_columns_calls: u64,
    pub scan_versioned_columns_calls: u64,
    pub change_stream_history_inspections: u64,
    pub analyze_calls: u64,
    pub flush_calls: u64,
    pub buffer_page_reads: u64,
    pub heap_backfill_pages: u64,
    pub heap_backfill_max_byte_end: u64,
    pub heap_scan_pages: u64,
    pub heap_scan_max_byte_end: u64,
    pub lsm_scan_block_bytes: u64,
}

thread_local! { static ACTIVITY: Cell<Activity> = Cell::new(Activity::default()); }

pub fn take() -> Activity {
    ACTIVITY.with(|value| value.replace(Activity::default()))
}

pub fn record(update: impl FnOnce(&mut Activity)) {
    ACTIVITY.with(|value| {
        let mut current = value.get();
        update(&mut current);
        value.set(current);
    });
}
