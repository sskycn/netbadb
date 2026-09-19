# NetbaDB deployment manifest v11

Deployment Manifest v11 is the only current `netbadbd` startup contract.
Versions 1 through 10 and future versions are rejected; there is no dual v10/v11
decoder. The manifest is strict, loaded once, and never watched or rewritten.

## Complete example

```json
{
  "version": 11,
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
    "allow_physical_design_receipt_read": false,
    "physical_index_admission": {"mode": "unadmitted"},
    "physical_columnar_snapshot_admission": {"mode": "unadmitted"},
    "physical_columnar_incremental_admission": {"mode": "unadmitted"}
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
v3 remain available; Manifest v11 does not change the 44-byte header, database
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

Apply permission alone never requires a receipt journal. A v11 daemon with
Index or Columnar apply enabled and no `mutation_receipts` still enforces its
configured admission mode and returns `receipt: null`.

## Operator component admission

Each present operator object requires three independent modes:
`physical_index_admission`, `physical_columnar_snapshot_admission`, and
`physical_columnar_incremental_admission`. There is no omission or null default.
The exact alternatives are `{"mode":"unadmitted"}` and:

```json
{
  "mode": "component_limits",
  "source_work_units": {"kind": "at_most", "maximum": 10000},
  "source_read_bytes": {"kind": "at_most", "maximum": 67108864},
  "prerequisite_work_units": {"kind": "unconstrained"},
  "prerequisite_read_bytes": {"kind": "unconstrained"},
  "prerequisite_write_bytes": {"kind": "at_most", "maximum": 16777216},
  "output_write_bytes": {"kind": "unconstrained"}
}
```

Every component is required. Constraints use exactly the tagged objects above;
shorthand numbers, nulls, missing or unknown fields/kinds/modes are rejected.
`maximum` is a u64 including zero and u64::MAX. The parser constructs Core
`PhysicalDesignMutationAdmissionLimits` and calls its validated policy constructor.
All six unconstrained is invalid; the typed Manifest error preserves the Core
policy error as its source. `unadmitted` instead selects the existing apply API
and explicitly preserves pre-Phase35 mutation behavior.

Component limits require the corresponding apply permission. Snapshot and
Incremental additionally require their corresponding `columnar_apply` allowed
mode. A disabled domain must say `unadmitted`. Enabled domains may independently
choose either mode; no domain inherits another domain's policy. There is no
operator admission builder override and embedded hosts retain their independent
per-call Phase34 policy authority.

A constrained NotProven bound rejects, even at u64::MAX. Equality passes.
Unconstrained components are outside the policy, never proven safe or zero.
A policy constraining only Snapshot LSM prerequisite writes is **partial
component admission**: it says nothing about source traversal or total mutation
cost. Components are never summed. Current initial Snapshot and Incremental
Columnar builds prove `output_write_bytes` for their one NBCS base plus NBCM,
including the second NBCS header write. Snapshot LSM also proves
`source_read_bytes` from the prospective post-flush SSTable extent. Index output
writes, whole-mutation work, CPU, memory, filesystem free space, and cumulative
quotas remain unproven.

`AtMost(M)` is conditional on current engine evidence. It is not a permanent
disable switch: a later engine may prove a component and admit it when the bound
fits the unchanged maximum. To disable a domain unconditionally, set
`allow_physical_columnar_apply` false or set the applicable
`columnar_apply.allow_snapshot` / `allow_incremental` permission false and use
the required `unadmitted` mode. A policy constraining only Columnar output is
still partial admission; it does not constrain source, prerequisites or memory.

Manifest parsing and `netbadb inspect` validate configuration, not whether current
data fits. They perform no mutation-work inspection or source scan and do not
open a Database solely to evaluate a bound. The sole worker recomputes current
inspection only when an explicitly approved apply still needs a mutation.

## Migration from v10

Without `operator`, change only `version` from 10 to 11. With `operator`, also
add exactly the three explicit `{"mode":"unadmitted"}` fields in the complete
example to preserve old behavior. There is no hidden migration default or dual
decoder. Selecting component limits is a separate deployment decision.
See [NBOP v7](server-operator-protocol-v7.md). NBOP v6 is historical and is
rejected by current binaries.
