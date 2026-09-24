# Storage contract rules

This crate contains implementation-independent storage descriptions moved from
`netbadb-storage`. In addition to `../AGENTS.md`, follow the ownership,
single-writer, error, and validation rules in
`../netbadb-storage/AGENTS.md`. Do not move concrete engine handles,
transaction managers, or table dispatch here.
