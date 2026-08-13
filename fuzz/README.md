# WAL recovery fuzzing

`wal_recovery` accepts at most 64 KiB, replaces the WAL belonging to a fresh
minimal heap, and calls `HeapStorage::open`. That public path invokes the
crate-private recovery decoder without adding a fuzz-only production API.
Root, alternate WAL, and heap files are isolated by process ID and removed
before and after every iteration.

Generate the small deterministic seed corpus and run a smoke fuzz with:

```bash
cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus
cargo +nightly fuzz run wal_recovery -- -runs=1000
```

The generated corpus contains empty input, a valid v3 header, Begin,
Begin+Commit, PageUpdate, and a structurally valid truncated final record. Do
not commit generated findings or large corpora.
