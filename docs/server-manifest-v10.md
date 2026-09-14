# NetbaDB deployment manifest v10

Deployment Manifest v10 is the only current `netbadbd` startup contract.
Versions 1 through 9 and future versions are rejected; there is no dual v9/v10
decoder. The manifest is strict, loaded once, and never watched or rewritten.

## Complete example

```json
{
  "version": 10,
  "listen": "127.0.0.1:7878",
  "authorization": {
    "local_plaintext": {"schema_admin": true, "tables": []},
    "clients": []
  },
  "tables": [{
    "path": "data/users.ndb",
    "id": 1,
    "name": "users",
    "columns": [
      {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": null,
       "nullable": false, "primary_key": true}
    ]
  }],
  "physical_design": {
    "evidence_limits": {
      "max_index_candidates": 64,
      "max_columnar_candidates": 64,
      "max_query_shapes_per_candidate": 32,
      "max_columnar_columns_per_candidate": 64
    },
    "advisor_policy": {
      "index": {"minimum_reports": 1, "minimum_distinct_query_shapes": 1,
                 "minimum_actual_scan_work_units": 0, "max_recommendations": 8},
      "columnar": {"minimum_reports": 1, "minimum_distinct_query_shapes": 1,
                    "minimum_actual_scan_work_units": 0, "max_recommendations": 8}
    },
    "columnar_apply": {
      "root": "data/columnar",
      "allow_snapshot": true,
      "allow_incremental": false
    },
    "mutation_receipts": {
      "path": "data/physical-design-receipts.nbmr",
      "max_file_bytes": 67108864
    }
  },
  "operator": {
    "unix_socket": "run/netbadb-operator.sock",
    "io_timeout_ms": 5000,
    "allow_physical_index_apply": true,
    "allow_physical_columnar_apply": true,
    "allow_physical_design_receipt_read": false
  }
}
```

## Mutation receipt journal

`physical_design.mutation_receipts` is optional. Omission alone disables the
manifest-derived journal. `null`, an `enabled` substitute, missing `path` or
`max_file_bytes`, and unknown fields are rejected. A relative path is resolved
from the directory containing the manifest, never the process working
directory. The path's parent must already exist, and an existing final object
must be a regular file rather than a directory, symlink, or other object.

`max_file_bytes` is passed unchanged to
`ServerPhysicalDesignMutationReceiptConfig::new`; the existing NBMR v3
minimum-capacity, canonical-parent, and final-object validation remains the
single authority. Manifest parsing and `netbadb inspect` perform that
validation without creating, opening, migrating, repairing, or reconciling a
journal. Those actions remain ordered worker-startup work after Database and
NBPC recovery and before daemon/operator readiness.

The journal is NBMR v3 for writes. The existing v1/v2 readers and migration to
v3 remain available; Manifest v10 does not change the 44-byte header, database
or journal incarnations, record framing, CRC, capacity, repair, or
reconciliation rules.

## Independent operator permission

Every present `operator` object must contain
`allow_physical_design_receipt_read`. This permission is independent of Index
and Columnar apply. Receipt status/list requests are rejected at the listener
when it is false, but apply behavior is unchanged. Conversely, receipt reads
may be true while both apply permissions are false.

Receipt read permission may be true only when the same manifest declares
`physical_design.mutation_receipts`; a programmatic builder cannot repair a
missing deployment declaration. When read permission is true, the manifest's
canonical path and capacity pin the externally visible journal. An explicit
builder receipt configuration is accepted only when both canonical values are
exactly equal; a mismatch fails Native and PostgreSQL startup. When read
permission is false, an embedded host may replace the manifest default with an
explicit builder configuration. A receipted apply response may still reveal
its opaque scoped reference, but that reference does not grant list access.

Apply permission alone never requires a receipt journal. A v10 daemon with
Index or Columnar apply enabled and no `mutation_receipts` preserves v9
behavior and returns `receipt: null`.

## Migration from v9

For a v9 manifest without `operator`, change `version` from 9 to 10 and leave
all other fields unchanged; omit `mutation_receipts`. No journal is created.

For a v9 manifest with `operator`, also add the required field:

```json
"allow_physical_design_receipt_read": false
```

Keeping it false and omitting `mutation_receipts` preserves v9 operator apply
behavior. Enabling reads is a separate deployment decision and requires the
complete receipt object above. See [NBOP v5](server-operator-protocol-v5.md).
