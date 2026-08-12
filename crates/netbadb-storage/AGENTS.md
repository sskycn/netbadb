# Storage and persistent-format rules

These rules apply to `netbadb-storage` in addition to the root and `crates/`
rules. Database files are untrusted input and storage correctness takes priority
over convenience or speculative performance.

## Persistent representation

- Never persist Rust memory layout, enum discriminants, pointers, `usize`, or
  host-endian values.
- Every persistent integer MUST have an explicit width and byte order.
- Every persistent root/header MUST identify the format with magic bytes and an
  explicit version. For an experimental format, a version encoded in the magic
  (for example `NBD1`) is acceptable when documented.
- Every page kind MUST have an explicit type tag. Reusing a tag with a new
  meaning is forbidden.
- Lengths, optional fields, page boundaries, and record boundaries MUST have
  explicit encodings and invariants.
- Checksums MAY be deferred for an experimental format, but the decision and
  corruption model MUST be documented before claiming durability or format
  stability.

The current file format is experimental. Before declaring any persistent format
stable, version handling and compatibility behavior MUST be explicit. Once
stable, field encodings, tags, and meanings MUST NOT change silently.

## Decoding and corruption

- Validate all externally controlled offsets, lengths, counts, tags, and page
  identifiers before indexing, slicing, allocation, or conversion.
- Use checked arithmetic when corrupt bytes can influence a size or offset.
- Reject truncated input, invalid UTF-8, unknown tags, impossible boundaries,
  and inconsistent metadata with bounded, diagnosable errors.
- Storage errors SHOULD include the operation and relevant file, page, slot, or
  offset context without dumping user row contents.
- Malformed files MUST NOT panic, trigger unbounded allocation, or cause memory
  unsafety.

Keep layout constants and codecs centralized. Encoding and decoding MUST be
symmetric and SHOULD be reviewed together.

## Page and ownership boundaries

- Page size, header size, slot count, used/free-space boundaries, page kind,
  and page identity MUST be explicit invariants.
- Prefer `PageId`, `FrameId`, `RowId`, and short-lived guards. Arbitrary page
  references MUST NOT escape into planner or executor layers.
- A future buffer pool MAY use RAII guards for pin/unpin and latches, but guard
  lifetimes SHOULD remain local to storage operations.
- Unsafe code requires a concrete measured need, a safe wrapper, and a
  `// SAFETY:` invariant. Do not introduce unsafe only for zero-copy decoding.

## Storage test baseline

The storage test suite as a whole SHOULD cover:

- encode/write/read/decode round trips;
- empty, minimum, maximum, and page-boundary values;
- full-page and multi-page insertion;
- close/reopen persistence;
- truncated files, invalid offsets, unknown tags, and corrupt metadata.

Each storage change MUST add or update tests for the invariants it changes; it
does not need to duplicate every baseline case. A fuzz- or property-test failure
MUST receive a deterministic regression test.
