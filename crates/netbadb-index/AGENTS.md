# Index subsystem rules

- Persistent B+Tree node encodings MUST use explicit versioned, fixed-width,
  little-endian fields. Rust layout and enum discriminants are not formats.
- Key and RowId comparison MUST be deterministic and explicit.
- This crate MUST NOT depend on storage, WAL, SQL, planner, or executor crates.
- Malformed node payloads MUST return typed errors without panicking, unbounded
  allocation, or unbounded traversal.
