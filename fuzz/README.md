# Storage decoding fuzzing

`wal_recovery` accepts at most 64 KiB, replaces the WAL belonging to a fresh
minimal heap, and calls `HeapStorage::open`. That public path invokes the
crate-private recovery decoder without adding a fuzz-only production API.
Root, alternate WAL, and heap files are isolated by process ID and removed
before and after every iteration.

`page_decode` accepts at most one 4096-byte page, zero-pads shorter inputs,
and exercises the public Page v4 header, slot, and record decoders. Its valid
seed uses `PageId(7)` because Page v4 CRC32C binds the expected logical page ID.

Generate the small deterministic seed corpus and run a smoke fuzz with:

```bash
cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus
cargo +nightly fuzz run wal_recovery -- -runs=1000
cargo +nightly fuzz run page_decode -- -runs=1000
```

The generated WAL corpus contains empty input, a valid v3 header, Begin,
Begin+Commit, a PageUpdate carrying Page v4 images, and a structurally valid
truncated final record. A separate legal Page v4 seed is generated for
`page_decode`. Do not commit generated findings or large corpora.
