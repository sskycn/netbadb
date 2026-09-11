# Adaptive Operations Phase 22

Phase 22 freezes deployment and local-operator presentation for the Phase 21
worker-owned physical-design advisor.

```text
Manifest v7
    -> Native or PostgreSQL daemon startup
    -> one worker-owned PhysicalDesignEvidenceWindow
    -> NBOP v2 explicit status/recommendation/rotation DTOs
    -> netbadb operator human presentation
```

Manifest v7 optionally supplies every evidence limit and both independent
recommendation policies. Omission disables design capture; there are no hidden
defaults, modes, hot reload, or environment variables. Programmatic builders
remain explicit replacements. Adaptive and Physical Design are independent
runtime dimensions and neither enables global visibility.

NBOP v2 nests the existing Adaptive status beside optional Physical Design
status, preserves Adaptive rotation and scheduler reset, and adds read-only
recommendations plus conditional design-evidence rotation. The two status
snapshots are individually worker-serialized, not cross-domain transactional.
The 64 KiB cap is unchanged; an oversized success becomes a bounded
`response_too_large` error without report truncation or evidence mutation.

The operator listener still owns no Database and remains one serial Unix-only
thread with a `0600` filesystem trust boundary. Recommendations always invoke
the Core advisor against current inventory, so ordinary SQL DDL can change a
later decision from Recommend to ExistingDesignCovers without a cache.

This phase adds no SQL, Native or PostgreSQL wire change, Inspection JSON
change, persistent-format change, metrics contract, recommendation persistence,
background work, scheduler call, maintenance, identity reservation, DDL
generation, projection build, or apply operation. Manifest v7 and NBOP v2 expose
advice; they do not turn advice into authority.
