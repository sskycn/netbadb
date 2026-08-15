#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"
cargo run -p netbadb-codegen -- go \
    --schema sdk/go/testschema/schema.json \
    --package testschema \
    --output sdk/go/testschema/netbadb_generated.go \
    --check
