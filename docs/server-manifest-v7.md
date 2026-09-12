# NetbaDB deployment manifest v7

Deployment Manifest v7 is the only current `netbadbd` startup configuration.
Versions 1 through 6 and future versions are rejected explicitly; there is no
dual v6/v7 decoder. This version is independent of Native Protocol v2,
PostgreSQL wire, Inspection JSON v7, SDK Schema Spec, Canonical Schema, and all
database persistent formats.

The manifest is strict and loaded once. Unknown fields fail decoding. It is
never watched or rewritten; policy changes require editing the file and
restarting the daemon. Runtime evidence, epochs, candidates, and recommendations
are never persisted back into it.

## Complete Driven + Physical Design + Operator example

Every numeric value below is an example, not a default. The example is parsed
by the server test suite.

```json
{
  "version": 7,
  "listen": "127.0.0.1:7878",
  "limits": {
    "max_connections": 128,
    "idle_timeout_ms": 300000,
    "write_timeout_ms": 30000,
    "max_result_rows": 100000
  },
  "authorization": {
    "local_plaintext": {
      "tables": [{"table_id":1,"read":true,"write":true,"transaction":true,"analyze":true}]
    },
    "clients": []
  },
  "tables": [{
    "path": "data/users.ndb",
    "id": 1,
    "name": "users",
    "columns": [{"id":1,"name":"id","physical_type":"uint64","semantic_type":"UserId","nullable":false,"primary_key":true}]
  }],
  "adaptive": {
    "mode": "driven",
    "feedback": {"limits":{"max_target_windows":16,"workload":{"max_query_shapes":64,"max_plan_variants_per_shape":8},"max_calibration_epochs":4,"max_calibration_query_shapes":64,"max_calibration_plan_variants_per_shape":8}},
    "host": {"tick_interval_ms":1000},
    "scheduler_policy": {"minimum_ticks_between_runs":1,"idle_retry_ticks":8,"no_progress_retry_ticks":8,"trial_retry_ticks":8},
    "orchestration_envelope": {
      "max_steps": 4,
      "per_step_maintenance_budget": {"max_work_units":100000,"max_read_bytes":16777216,"max_write_bytes":16777216,"max_actions":1},
      "run_maintenance_budget": {"max_work_units":400000,"max_read_bytes":67108864,"max_write_bytes":67108864,"max_actions":4}
    },
    "scope": {"table_ids":[1],"calibration_classes":["seq_scan","index_point","index_range","columnar"]},
    "automatic_policy": {
      "safe_mode": {
        "allow_columnar_maintenance": true,
        "allow_planner_calibration": true,
        "adaptive_policy": {"minimum_expected_benefit_work_units":1,"minimum_keep_benefit_work_units":1},
        "workload_policy": {"minimum_samples":3,"minimum_actual_work_units":1,"minimum_distinct_visibility_points":2,"minimum_keep_improvement_work_units":1,"maximum_tolerated_regression_work_units":0},
        "planner_calibration_policy": {
          "minimum_samples":8,"minimum_actual_work_units":1,"minimum_distinct_visibility_points":2,
          "minimum_distinct_query_shapes":3,"minimum_directional_query_shape_margin":2,
          "error_deadband_work_units":1,"minimum_shadow_error_improvement_work_units":1,
          "global_min_ratio":{"numerator":1,"denominator":2},"global_max_ratio":{"numerator":2,"denominator":1},
          "maximum_step_up_ratio":{"numerator":9,"denominator":8},"maximum_step_down_ratio":{"numerator":9,"denominator":8}
        },
        "calibration_trial_policy": {"minimum_samples":8,"minimum_actual_work_units":1,"minimum_distinct_visibility_points":2,"minimum_distinct_query_shapes":3,"minimum_keep_error_improvement_work_units":1,"maximum_tolerated_error_regression_work_units":0}
      },
      "allow_columnar_compaction": true,
      "allow_change_stream_gc": true,
      "allow_lsm_flush": true,
      "allow_lsm_compaction": true,
      "change_stream_gc_policy": {"minimum_reclaimable_batches":16,"minimum_reclaimable_bytes":1048576},
      "lsm_flush_policy": {"minimum_memtable_bytes":0},
      "lsm_compaction_policy": {"minimum_input_bytes":0},
      "columnar_compaction_policy": {"minimum_delta_segments":1,"minimum_delta_bytes":0},
      "cross_lane_service": {"mode":"bounded_four_lane_cycle"},
      "max_candidate_tables":16,"max_calibration_classes":4,"max_fairness_entries":64
    }
  },
  "physical_design": {
    "evidence_limits": {"max_index_candidates":64,"max_columnar_candidates":64,"max_query_shapes_per_candidate":32,"max_columnar_columns_per_candidate":64},
    "advisor_policy": {
      "index": {"minimum_reports":3,"minimum_distinct_query_shapes":2,"minimum_actual_scan_work_units":1000,"max_recommendations":8},
      "columnar": {"minimum_reports":3,"minimum_distinct_query_shapes":2,"minimum_actual_scan_work_units":1000,"max_recommendations":8}
    }
  },
  "operator": {"unix_socket":"run/netbadb-operator.sock","io_timeout_ms":5000}
}
```

## Migration from v6

The no-design migration is mechanical:

```text
change "version": 6 to "version": 7
leave every other field unchanged
omit "physical_design"
```

That preserves Server, Adaptive, and Operator behavior and leaves Physical
Design disabled. Version 6 itself is rejected by current binaries.

## Existing deployment fields

`listen`, `limits`, `tls`, `authorization`, `tables`, and `adaptive` retain v6
meaning. Plaintext is loopback-only; non-loopback Native listeners require
mutual TLS. PostgreSQL mode remains plaintext loopback. Relative table, TLS,
and operator paths resolve from the manifest directory. Tables are exact subset
expectations for an already installed catalog: startup does not create or repair
database files, enable global visibility, or create a coordinator.

Limits retain their bounded typed validation. Authorization remains transport
specific and schema-bound. The complete Adaptive object and all its nested
limits, scheduler policy, orchestration envelope, scope, and automatic policy
remain explicit; omitting `adaptive` is its sole disabled representation.
Feedback-only and driven semantics are unchanged from v6.

## Physical Design

Omitting `physical_design` is the sole disabled representation. `null`, an
`enabled` flag, or a `mode` field is invalid. Presence enables the worker-owned
evidence/advisor runtime and its embedded-host programmatic controls. It does
not expose apply through the manifest, operator, daemon, CLI, SQL, Native, or
PostgreSQL wire protocols. Every nested object is strict and every field is
required. There are no manifest defaults.

`evidence_limits` maps one-for-one to `PhysicalDesignEvidenceLimits`:
`max_index_candidates`, `max_columnar_candidates`,
`max_query_shapes_per_candidate`, and
`max_columnar_columns_per_candidate`. `advisor_policy.index` and
`advisor_policy.columnar` independently map to
`PhysicalDesignRecommendationPolicy`, each with `minimum_reports`,
`minimum_distinct_query_shapes`, `minimum_actual_scan_work_units`, and
`max_recommendations`. The final value is constructed through
`ServerPhysicalDesignAdvisorConfig::new`.

Zero capacities and thresholds retain Core semantics, including capacity
truncation, inconclusive advice, and zero recommendations; the manifest layer
adds no minimum and performs no reinterpretation.

Adaptive and Physical Design are independent. Enabling Physical Design alone
does not create an Adaptive evidence pool or scheduler. Neither section enables
global visibility. On a LegacyLocal database, queries still succeed while
design capture may record `global_visibility_required`, leaving recommendations
with no evidence.

For library embedding, `with_physical_design_advisor` is an explicit replacement
for the manifest-derived value. Adaptive builders replace only the Adaptive
dimension; the Physical Design builder replaces only the design dimension.
Native and PostgreSQL daemon startup honor the same manifest value.

## Operator and tools

`operator` is legal when at least one controllable runtime is present:
Adaptive, Physical Design, or both. It is rejected when both are absent.
Physical Design without an operator remains legal because an embedded caller
may use `ServerHandle::physical_design_control()`.

The operator socket remains Unix-only, mode `0600`, one serial listener thread,
and filesystem-authenticated. Its machine contract is independently versioned
as [NBOP v2](server-operator-protocol-v2.md). It cannot change policy; threshold
and limit changes require restart.

`netbadbd --manifest server.json` and `netbadbd --manifest server.json
--postgres` create the configured design runtime before readiness. The bounded
readiness line reports only `physical-design enabled` or `physical-design
disabled`, never policy or candidates.

`netbadb inspect` parses and fully validates Manifest v7 through
`ServerConfig::from_manifest_path`, but starts no server, socket, evidence
window, query, or advisor. Inspection does not require a daemon online.

Runtime evidence is memory-only and is lost at restart. Manifest v7 adds no
recommendation persistence, hot reload, background advisor, design automation,
scheduler invocation, identity reservation, DDL generation, `CREATE INDEX`,
projection build, apply authority, schema mutation, or publication of global
visibility. Heap, BTree and Index Catalog, LSM, Columnar and Projection Catalog,
Change Stream, Coordinator, and every persistent format remain unchanged.
