# Row codec rules

This crate owns an on-disk row payload encoding that was moved from
`netbadb-storage`. In addition to `../AGENTS.md`, follow all applicable
persistent-representation and corruption rules in
`../netbadb-storage/AGENTS.md`. Changes must preserve existing scalar tags,
field order, NULL encoding, widths, and byte order unless a separately
reviewed format version is introduced.
