# Integration tests

Cross-crate behavior currently lives next to the public `netbadb-core` API in
`crates/netbadb-core/tests/embedded.rs`. This directory is reserved for tests
that need a repository-wide fixture or protocol process.
