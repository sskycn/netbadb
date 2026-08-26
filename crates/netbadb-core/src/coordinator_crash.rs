//! Test-only abrupt termination hooks for coordinator crash-window proofs.

use std::ffi::OsStr;

pub(crate) const CHILD_ENV: &str = "NETBADB_COORDINATOR_CRASH_CHILD";
pub(crate) const CASE_ENV: &str = "NETBADB_COORDINATOR_CRASH_CASE";
pub(crate) const ROOT_ENV: &str = "NETBADB_COORDINATOR_CRASH_ROOT";
const POINT_ENV: &str = "NETBADB_COORDINATOR_CRASH_POINT";
pub(crate) const EXIT_CODE: i32 = 87;

pub(crate) fn maybe_crash(point: &str) {
    if enabled(point) {
        std::process::exit(EXIT_CODE);
    }
}

pub(crate) fn maybe_crash_indexed(prefix: &str, index: usize) {
    if std::env::var_os(CHILD_ENV).as_deref() != Some(OsStr::new("1")) {
        return;
    }
    let configured = std::env::var(POINT_ENV).unwrap_or_default();
    if configured == format!("{prefix}-{index}") {
        std::process::exit(EXIT_CODE);
    }
}

pub(crate) fn enabled(point: &str) -> bool {
    std::env::var_os(CHILD_ENV).as_deref() == Some(OsStr::new("1"))
        && std::env::var_os(POINT_ENV).as_deref() == Some(OsStr::new(point))
}

pub(crate) fn configure_child(
    command: &mut std::process::Command,
    case: &str,
    root: &std::path::Path,
    point: &str,
) {
    command
        .env(CHILD_ENV, "1")
        .env(CASE_ENV, case)
        .env(ROOT_ENV, root)
        .env(POINT_ENV, point);
}
