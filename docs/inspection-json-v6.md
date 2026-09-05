# Inspection JSON v6

Inspection JSON v6 is emitted only when a statement plan contains
`ColumnarScan`. Earlier plans retain their existing version: ordinary plans use
v3, partition plans v4, and index nested-loop plans v5.

The new `columnar_scan` operator contains logical binding/table identity,
required source columns, stable projection ID, projection generation, and
source storage ID. It does not expose paths, segment offsets, file handles,
snapshot sequence internals, or mutable storage state. Projection health and
lifecycle remain embedded Core inspection APIs in Phase 1; the offline CLI has
no automatic projection-location catalog from which to attach derived files.
