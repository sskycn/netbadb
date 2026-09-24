use std::ffi::OsStr;

pub(crate) const CHILD_ENV: &str = "NETBADB_TEST_CRASH_CHILD";
#[cfg(test)]
pub(crate) const CASE_ENV: &str = "NETBADB_TEST_CRASH_CASE";
#[cfg(test)]
pub(crate) const DATABASE_PATH_ENV: &str = "NETBADB_TEST_DB_PATH";
const POINT_ENV: &str = "NETBADB_TEST_CRASH_POINT";
pub(crate) const EXIT_CODE: i32 = 86;

thread_local! { static SUPPRESSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
pub(crate) fn without_crash<T>(operation: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            SUPPRESSED.set(self.0);
        }
    }
    let _reset = Reset(SUPPRESSED.replace(true));
    operation()
}

pub(crate) fn maybe_crash_named(point: &str) {
    if !SUPPRESSED.get()
        && std::env::var_os(CHILD_ENV).as_deref() == Some(OsStr::new("1"))
        && std::env::var_os(POINT_ENV).as_deref() == Some(OsStr::new(point))
    {
        std::process::exit(EXIT_CODE);
    }
}

pub(crate) fn maybe_crash_indexed(prefix: &str, position: usize) {
    maybe_crash_named(&format!("{prefix}-{position}"));
}

#[cfg(test)]
pub(crate) fn configure_named_child(
    command: &mut std::process::Command,
    case: &str,
    path: &std::path::Path,
    point: &str,
) {
    command
        .env(CHILD_ENV, "1")
        .env(CASE_ENV, case)
        .env(DATABASE_PATH_ENV, path)
        .env(POINT_ENV, point);
}
