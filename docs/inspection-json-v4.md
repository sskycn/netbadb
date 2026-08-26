# Inspection JSON v4

Inspection JSON v4 extends the current inspection projection only when a
catalog or statement contains RANGE partition metadata. Existing unpartitioned
catalogs and plans continue to render the historical v3 envelope and shape;
v1, v2, and v3 documents are not reinterpreted.

A partitioned table adds a `placement` object with:

- `kind: "range_partitioned"`;
- numeric partition-key ColumnId;
- canonical range-order entries containing PartitionId and optional typed
  lower/upper scalar bounds.

The physical plan adds `operator: "partitioned_scan"` with logical table and
relation-binding identity, requested columns, partition key, total partition
count, and the exact selected partitions. Each selected partition reports its
PartitionId and one local `seq_scan`, `index_scan`, or `range_index_scan`
choice. Paths, storage handles, heap RowIds, pages, WAL, and coordinator
internals remain hidden.
