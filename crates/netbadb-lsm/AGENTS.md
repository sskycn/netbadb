# LSM rules

Follow `../AGENTS.md` and the persistent-format, corruption, single-writer,
WAL-before-data, and recovery rules in `../netbadb-storage/AGENTS.md`. This
crate owns its WAL, manifest, SSTables, transaction lifecycle, and recovery;
it must not depend on Heap, Columnar, Core, or the storage facade.
