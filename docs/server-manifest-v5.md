# NetbaDB deployment manifest v5

Deployment manifest v5 is the only current `netbadbd` startup configuration.
Versions 1 through 4 and future versions are rejected explicitly; there is no
dual v4/v5 decoder. The manifest is a deployment contract, not a database file
format, canonical schema, wire protocol, or runtime-status document.

V5 retains v4's `listen`, `limits`, `tls`, `authorization`, and `tables`
semantics exactly and adds the optional top-level `adaptive` object. All
objects are strict: unknown fields, modes, calibration classes, or cross-lane
tags fail JSON decoding. Configuration is loaded once at startup and is never
rewritten or watched.

## Migration from v4

The non-adaptive migration is deliberately mechanical:

```text
change "version": 4 to "version": 5
leave every other field unchanged
omit "adaptive"
```

The resulting server has the same runtime behavior and Adaptive is Disabled.
Enabling Adaptive is a separate, explicit deployment change.

## Complete driven example

The following example shows every v5 Adaptive field. Numeric values are
examples, not defaults.

```json
{
  "version": 5,
  "listen": "127.0.0.1:7878",
  "limits": {
    "max_connections": 128,
    "idle_timeout_ms": 300000,
    "write_timeout_ms": 30000,
    "max_result_rows": 100000
  },
  "authorization": {
    "local_plaintext": {
      "tables": [
        {
          "table_id": 1,
          "read": true,
          "write": true,
          "transaction": true,
          "analyze": true
        }
      ]
    },
    "clients": []
  },
  "tables": [
    {
      "path": "data/users.ndb",
      "id": 1,
      "name": "users",
      "columns": [
        {
          "id": 1,
          "name": "id",
          "physical_type": "uint64",
          "semantic_type": "UserId",
          "nullable": false,
          "primary_key": true
        }
      ]
    }
  ],
  "adaptive": {
    "mode": "driven",
    "feedback": {
      "limits": {
        "max_target_windows": 16,
        "workload": {
          "max_query_shapes": 64,
          "max_plan_variants_per_shape": 8
        },
        "max_calibration_epochs": 4,
        "max_calibration_query_shapes": 64,
        "max_calibration_plan_variants_per_shape": 8
      }
    },
    "host": {
      "tick_interval_ms": 1000
    },
    "scheduler_policy": {
      "minimum_ticks_between_runs": 1,
      "idle_retry_ticks": 8,
      "no_progress_retry_ticks": 8,
      "trial_retry_ticks": 8
    },
    "orchestration_envelope": {
      "max_steps": 4,
      "per_step_maintenance_budget": {
        "max_work_units": 100000,
        "max_read_bytes": 16777216,
        "max_write_bytes": 16777216,
        "max_actions": 1
      },
      "run_maintenance_budget": {
        "max_work_units": 400000,
        "max_read_bytes": 67108864,
        "max_write_bytes": 67108864,
        "max_actions": 4
      }
    },
    "scope": {
      "table_ids": [1],
      "calibration_classes": [
        "seq_scan",
        "index_point",
        "index_range",
        "columnar"
      ]
    },
    "automatic_policy": {
      "safe_mode": {
        "allow_columnar_maintenance": true,
        "allow_planner_calibration": true,
        "adaptive_policy": {
          "minimum_expected_benefit_work_units": 1,
          "minimum_keep_benefit_work_units": 1
        },
        "workload_policy": {
          "minimum_samples": 3,
          "minimum_actual_work_units": 1,
          "minimum_distinct_visibility_points": 2,
          "minimum_keep_improvement_work_units": 1,
          "maximum_tolerated_regression_work_units": 0
        },
        "planner_calibration_policy": {
          "minimum_samples": 8,
          "minimum_actual_work_units": 1,
          "minimum_distinct_visibility_points": 2,
          "minimum_distinct_query_shapes": 3,
          "minimum_directional_query_shape_margin": 2,
          "error_deadband_work_units": 1,
          "minimum_shadow_error_improvement_work_units": 1,
          "global_min_ratio": { "numerator": 1, "denominator": 2 },
          "global_max_ratio": { "numerator": 2, "denominator": 1 },
          "maximum_step_up_ratio": { "numerator": 9, "denominator": 8 },
          "maximum_step_down_ratio": { "numerator": 9, "denominator": 8 }
        },
        "calibration_trial_policy": {
          "minimum_samples": 8,
          "minimum_actual_work_units": 1,
          "minimum_distinct_visibility_points": 2,
          "minimum_distinct_query_shapes": 3,
          "minimum_keep_error_improvement_work_units": 1,
          "maximum_tolerated_error_regression_work_units": 0
        }
      },
      "allow_columnar_compaction": true,
      "allow_change_stream_gc": true,
      "allow_lsm_flush": true,
      "allow_lsm_compaction": true,
      "change_stream_gc_policy": {
        "minimum_reclaimable_batches": 16,
        "minimum_reclaimable_bytes": 1048576
      },
      "lsm_flush_policy": {
        "minimum_memtable_bytes": 0
      },
      "lsm_compaction_policy": {
        "minimum_input_bytes": 0
      },
      "columnar_compaction_policy": {
        "minimum_delta_segments": 1,
        "minimum_delta_bytes": 0
      },
      "cross_lane_service": {
        "mode": "bounded_four_lane_cycle"
      },
      "max_candidate_tables": 16,
      "max_calibration_classes": 4,
      "max_fairness_entries": 64
    }
  }
}
```

Relative Heap and TLS paths resolve from the manifest directory. Tables remain
required exact subset expectations for existing installed catalog resources;
ordinary startup never creates a database, repairs a missing catalog, enables
global visibility, or creates a coordinator. TLS, authorization, table grants,
schema-admin, and listener-security behavior are unchanged from
[manifest v4](server-manifest-v4.md).

## Adaptive modes

Omitting `adaptive` is the sole Disabled representation. `"adaptive": null`
and `"mode": "disabled"` are invalid. When `adaptive` is present, exactly two
tagged modes exist.

Feedback-only requires the complete evidence-limit snapshot:

```json
"adaptive": {
  "mode": "feedback_only",
  "feedback": {
    "limits": {
      "max_target_windows": 16,
      "workload": {
        "max_query_shapes": 64,
        "max_plan_variants_per_shape": 8
      },
      "max_calibration_epochs": 4,
      "max_calibration_query_shapes": 64,
      "max_calibration_plan_variants_per_shape": 8
    }
  }
}
```

These values construct `AdaptiveEvidencePoolLimits` and
`AdaptiveWorkloadLimits` one-for-one, then `ServerAdaptiveFeedbackConfig`.
Core remains the authority for zero limits, capacity behavior, window
rotation, and truncation. Feedback-only captures eligible authorized
autocommit queries but starts no host cadence or scheduler.

Driven requires `feedback`, `host`, `scheduler_policy`,
`orchestration_envelope`, `scope`, and `automatic_policy`. There are no presets
or environment-dependent defaults. `host.tick_interval_ms` becomes a
`Duration`; `AutomaticSchedulerPolicy::new` validates all four tick fields.
`max_steps` and both budgets map directly to
`AutomaticOrchestrationEnvelope` and `MaintenanceBudget::new`. Physical budget
values, including `max_actions: 0`, are not reinterpreted by the manifest;
calibration-only orchestration may legally have zero physical budget.

Scope contains canonical `TableId` integers and only these exact calibration
class tags:

| JSON tag | Runtime class |
| --- | --- |
| `seq_scan` | `SeqScan` |
| `index_point` | `IndexPoint` |
| `index_range` | `IndexRange` |
| `columnar` | `Columnar` |

Aliases and duplicate table/class entries are rejected. Scope cardinality and
other structural policy constraints are validated by
`ServerAdaptiveDriverConfig::new`. Unknown committed TableIds fail after the
Database opens but before the worker reports readiness or the listener binds.

## Automatic policy mapping

Every displayed `automatic_policy` and nested `safe_mode` field is required,
even when its corresponding `allow_*` flag is false. The file is a complete
policy snapshot, so an enable flag never changes JSON shape. The fields map
directly to `AutomaticMultiSafeModePolicy`, `AutomaticSafeModePolicy`,
`AdaptivePolicy`, `AdaptiveWorkloadPolicy`, `PlannerCalibrationPolicy`, and
`AutomaticCalibrationTrialPolicy`.

Planner ratios are positive integer objects, never floating point:

```json
{ "numerator": 1, "denominator": 2 }
```

`CalibrationRatio::new` performs validation and canonicalization, so `2/4`
has the runtime meaning `1/2`. Core's shared planner-policy validation rejects
`global_min_ratio > global_max_ratio` and step-up or step-down ratios below
one during manifest conversion, before the first background tick.

Columnar compaction, Change Stream GC, LSM flush, and LSM compaction policy
fields use the units shown by their names: segment counts or bytes. Existing
Core validators remain authoritative. In particular, an enabled Columnar
compaction policy needs a nonzero segment or byte threshold, and enabled
Change Stream GC needs a nonzero batch or byte threshold.

The cross-lane service is one of these exact tagged objects:

```json
{ "mode": "strict_physical_priority" }
```

```json
{
  "mode": "bounded_columnar_burst",
  "max_consecutive_columnar_admissions": 4
}
```

```json
{ "mode": "bounded_four_lane_cycle" }
```

Variant-specific fields are strict. A zero bounded burst is invalid, and a
burst-count field on either other mode is an unknown field.

## Runtime and operator behavior

`ServerConfig` stores the runtime-ready selected mode. `TcpServer::new` and
`PostgresTcpServer::new` use it identically. An explicit programmatic
`with_adaptive_feedback` or `with_adaptive_driver` call replaces the
manifest-derived mode; the last explicit builder call wins and creates only
one pool/scheduler composition.

`netbadbd --manifest PATH` and `netbadbd --manifest PATH --postgres` need no
Adaptive flag or environment variable. Their bounded startup diagnostic says
`adaptive disabled`, `adaptive feedback-only`, or `adaptive driven`. It is
human operational output, not a versioned wire or JSON interface.

Driven mode does not enable global visibility. On a LegacyLocal database,
feedback may report `GlobalVisibilityRequired` and Core decides whether any
automatic work is safe. Adaptive scope is operator maintenance policy, not a
principal grant, and only already-authorized queries contribute feedback.

Runtime evidence, scheduler gates, ticks, trials, faults, and status are never
persisted in the manifest. Restart creates a fresh W0 evidence pool, scheduler,
and T1 host clock; already committed physical maintenance remains committed.
There is no hot reload, filesystem watcher, dynamic scope, periodic status
logger, automatic evidence rotation, or automatic fault reset.

The embedded `ServerHandle` control API retains `status`, `rotate_evidence`,
and `reset_faulted_scheduler`. `netbadbd` has no live admin transport in v5.
An operator currently recovers a daemon stuck awaiting explicit renewal, or a
faulted scheduler, by restarting the process when schema-driven renewal does
not apply. Phase 17 intentionally adds no HTTP, Unix socket, Native/PG admin
frame, SQL command, function, or signal protocol.

`netbadb inspect` uses the same v5 parser and validates the complete Adaptive
section, including constructors and structural rules, before offline
inspection. It never creates an evidence pool, host cadence, scheduler, or
maintenance run and does not gain Adaptive-specific database mutations beyond
the existing normal open/recovery semantics.

V5 changes only the Server Deployment Manifest version. Native Protocol v2,
PostgreSQL wire, SQL, authorization, SessionPolicy, Server metrics, Inspection
JSON v7, Canonical Schema, Heap, BTree, LSM, Columnar, Change Stream,
Coordinator, SDK Schema Spec, and all persistent database formats are
unchanged.
