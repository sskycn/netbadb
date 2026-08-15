#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cargo build --manifest-path "$repository_root/Cargo.toml" -p netbadb-server --example go_sdk_fixture
cd "$repository_root/sdk/go"
NETBADB_GO_FIXTURE_BIN="$repository_root/target/debug/examples/go_sdk_fixture" go test -count=1 -tags=integration ./...
