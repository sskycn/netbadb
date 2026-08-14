# Protocol subsystem rules

- Every wire layout MUST use explicit versioned tags, fixed-width integers, and
  documented little-endian fields. Rust layout and enum discriminants are not
  protocol contracts.
- All untrusted lengths, counts, flags, tags, and UTF-8 strings MUST be bounded
  and validated before allocation or indexing.
- Malformed or truncated input MUST return typed errors and MUST NOT panic,
  allocate without a protocol bound, or loop without progress.
- Protocol versions, message tags, scalar/type tags, and error codes are wire
  compatibility contracts. Changing them requires an explicit compatibility
  decision and updated golden-byte tests.
- This crate MUST remain transport- and execution-independent. It may depend on
  shared wire domain types, but not core, server, executor, planner, or storage.
