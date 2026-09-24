# Heap transaction storage boundary

Follow `../AGENTS.md` and all persistent-format, recovery, page, WAL, and mutation-ownership rules in `../netbadb-storage/AGENTS.md`. This crate owns Heap authority and never depends on the storage facade.
