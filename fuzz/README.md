# Storage decoding fuzzing

`wal_recovery` accepts at most 64 KiB, replaces the WAL belonging to a fresh
minimal heap, and calls `HeapStorage::open`. That public path invokes the
crate-private recovery decoder without adding a fuzz-only production API.
Root, alternate WAL, and heap files are isolated by process ID and removed
before and after every iteration.

`page_decode` accepts at most one 4096-byte page, zero-pads shorter inputs,
and exercises the public Page v5 header, generation-aware slot-state, and
record decoders. Its valid seed uses `PageId(7)` because the CRC32C binds the
expected logical page ID.

`btree_decode` accepts at most one 4060-byte index payload plus a one-byte node
selector and directly exercises the public versioned Meta, Leaf, and Internal
decoders with a nullable UInt64 `IndexSpec`. Arbitrary bytes must return a node
or typed `IndexError` without panicking, unbounded allocation, or traversal.

Generate the small deterministic seed corpus and run a smoke fuzz with:

```bash
cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus
cargo +nightly fuzz run wal_recovery -- -runs=1000
cargo +nightly fuzz run page_decode -- -runs=1000
cargo +nightly fuzz run btree_decode -- -runs=1000
```

The generated WAL corpus contains empty input, a valid v3 header, Begin,
Begin+Commit, a PageUpdate carrying Page v5 images, and a structurally valid
truncated final record. A separate legal Page v5 seed is generated for
`page_decode`. The B+Tree corpus adds empty input, valid metadata,
empty/one-entry leaves, one internal separator, and a truncated leaf. Do not
commit generated findings or large corpora.
