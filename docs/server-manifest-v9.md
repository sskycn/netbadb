# NetbaDB deployment manifest v9

> Historical deployment contract. Current `netbadbd` requires Manifest v10;
> v9 is explicitly rejected. The remainder of this document preserves the v9
> contract as shipped.

Deployment Manifest v9 is the only current `netbadbd` startup configuration.
Versions 1 through 8 are not decoded by compatibility shims: versions 1 through
8 and future versions are rejected explicitly. The manifest is strict, loaded
once, and never watched or rewritten.

## Operator-approved physical design

The `operator.allow_physical_columnar_apply` field is required whenever an
operator object is present. It is a second gate, separate from the physical
placement policy:

```json
{
  "version": 9,
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
    }
  },
  "operator": {
    "unix_socket": "run/netbadb-operator.sock",
    "io_timeout_ms": 5000,
    "allow_physical_index_apply": false,
    "allow_physical_columnar_apply": true
  }
}
```

`physical_design.columnar_apply` is optional. When present it is a strict
object: `root`, `allow_snapshot`, and `allow_incremental` are all required and
unknown fields are rejected. At least one mode must be enabled. A relative
root is resolved from the manifest directory; the resolved root must already
exist and be a directory. Startup never creates it. The Server stores the
canonical root privately and never places it on the operator wire.

If the operator grants Columnar apply, the manifest policy is authoritative.
Any programmatic builder override must equal the same canonical root and mode
allowlist; a mismatch fails startup. If the operator does not grant Columnar
apply, a programmatic host may use its own explicit Columnar policy.

The operator permission is not an automatic action. The daemon does not enable
Change Streams, run maintenance, schedule applies, retry requests, rotate
evidence, or persist operator approvals. Incremental apply therefore requires
an already enabled and healthy Change Stream at the current evidence epoch.

## Migration from v8

Manifest v8 is historical and is not accepted by the current daemon. A
deployment must be rewritten as v9. The mechanical portion is changing the
version from `8` to `9` and adding
`"allow_physical_columnar_apply": false` to every operator object. To grant
Columnar approval, add the complete `physical_design.columnar_apply` object and
set the operator field to `true`; the placement root must be provisioned before
startup.

The existing v8 `allow_physical_index_apply` field remains required and
independent. Setting either apply permission to `true` still requires the
corresponding Physical Design configuration.

See [NBOP v4](server-operator-protocol-v4.md) for the local approval request
and response contract.
