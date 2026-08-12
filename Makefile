MSRV := 1.85.0

.PHONY: fmt fmt-check check clippy test msrv-check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

check:
	cargo check --workspace --all-targets

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

msrv-check:
	cargo +$(MSRV) check --workspace --all-targets
