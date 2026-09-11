# Adaptive Operations Phase 17: deployment manifest v5 and daemon enablement

## Mission

Phase 16 proved the Server runtime driver. Phase 17 freezes how an operator
selects that existing runtime through one strict deployment contract:

```text
deployment manifest v5
    -> typed manifest-only decode structures
    -> existing Core and Server constructors
    -> ServerConfig startup mode
    -> Native or PostgreSQL Database worker
```

The manifest describes policy, never runtime state. It does not become another
evidence, scheduler, orchestration, lane-service, or maintenance authority.

## Version and mode contract

Manifest v5 is the only accepted current version. V4 is historical and is
rejected rather than decoded alongside v5. Changing an otherwise valid v4
file's version to 5 and omitting `adaptive` preserves Disabled behavior.

The optional top-level Adaptive section has two explicit tagged forms:

```text
absent                         -> Disabled
mode = feedback_only          -> FeedbackOnly
mode = driven                 -> Driven
```

Null and a redundant disabled tag are rejected. Every enabled-mode field is
required, every nested object denies unknown fields, and all mode, calibration
class, and cross-lane tags are closed enums. No Core `Default` fills omitted v5
Adaptive policy.

Feedback limits map directly to the Phase 15 pool configuration. Driven adds a
millisecond host interval, Phase 13 scheduler policy, Phase 12 envelope and
budgets, explicit TableId/calibration-class scope, and the complete Phase 11
multi-safe-mode policy. Integer ratio pairs go through
`CalibrationRatio::new`; scheduler policy goes through
`AutomaticSchedulerPolicy::new`; structural configuration ends at
`ServerAdaptiveDriverConfig::new`.

Planner calibration ratio relationships previously validated only on the
advisor path are exposed through `PlannerCalibrationPolicy::is_valid`. Both
the advisor and the Server driver constructor use that Core helper. An invalid
deployment is therefore rejected before the first tick without copying the
formula into manifest code.

## Startup ownership and overrides

`ServerConfig` owns the runtime-ready manifest mode. Both `TcpServer::new` and
`PostgresTcpServer::new` use it by default. Existing explicit builder methods
remain replacement operations: the last `with_adaptive_feedback` or
`with_adaptive_driver` call wins over the manifest and exactly one adaptive
composition reaches the worker.

Database open and unknown committed-TableId validation still complete before
worker readiness and listener bind. No manifest mode enables global visibility
or creates a coordinator. LegacyLocal continues under the Phase 16 theorem:
feedback may require global visibility, while Core alone decides whether work
can proceed.

`netbadbd` needs no new flag or environment variable. Both native startup and
`--postgres` consume the same `ServerConfig`; startup stderr includes only a
bounded human mode label. It does not dump policies, TLS material,
authorization identities, SQL, or evidence.

## Inspection and lifecycle

`netbadb inspect` shares the parser, so malformed or semantically invalid
Adaptive policy fails before inspection. The CLI extracts only table bootstrap
data and opens the offline Database through its existing recovery path. It
never instantiates Adaptive feedback, cadence, scheduling, or maintenance.

Configuration is loaded once. Runtime status, evidence windows, logical ticks,
trial state, and scheduler gates are neither persisted nor written back.
Restart creates a fresh pool/scheduler/host clock while completed physical
maintenance remains durable.

Phase 17 adds no daemon admin transport. The embedded control handle still
provides status, explicit evidence rotation, and faulted-scheduler reset. A
daemon operator currently uses process restart when explicit renewal or fault
recovery is required and no schema-driven cohort advance applies. The server
does not auto-rotate evidence or auto-reset faults.

## Compatibility

Only the deployment manifest contract changes from v4 to v5. Native Protocol
v2, PostgreSQL wire, SQL, SessionPolicy, authorization, Server metrics,
Inspection JSON v7, SDK Schema Spec, and every database persistent format are
unchanged. See [the complete v5 contract](server-manifest-v5.md).
