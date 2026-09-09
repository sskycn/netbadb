use netbadb_types::{ColumnId, ColumnarProjectionId, ScalarValue, TableId};

use crate::adaptive_workload_tests::TimelineFixture;
use crate::execution_feedback_tests::TABLE_ID;
use crate::{
    AdaptiveColumnarCompactionAbortReason, AdaptiveColumnarCompactionDecision,
    AdaptiveColumnarCompactionNoActionReason, AdaptiveColumnarCompactionOutcome,
    AdaptiveColumnarCompactionPolicy, AdaptivePolicy, ColumnarAdvanceBudget,
    ColumnarProjectionHealth, ColumnarProjectionSpec, Database, MaintenanceAction,
    MaintenanceBlocker, MaintenanceBound, MaintenanceBudget, MaintenanceOutcome,
};

const CLOCK_TABLE_ID: TableId = TableId(88_002);

fn generous_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1)
}

fn projection_id(database: &Database) -> ColumnarProjectionId {
    database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection identity")
}

fn create_fresh_delta(database: &mut Database) -> ColumnarProjectionId {
    let projection_id = projection_id(database);
    database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create source change");
    let advance = database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
        .expect("advance projection into a fresh Delta");
    assert!(advance.caught_up);
    let projection = &database.inspect_columnar_projections()[0];
    assert_eq!(projection.health, ColumnarProjectionHealth::Fresh);
    assert!(projection.delta_segment_count.unwrap_or(0) > 0);
    projection_id
}

fn compaction_proposal(
    database: &Database,
    compaction_policy: AdaptiveColumnarCompactionPolicy,
    adaptive_policy: AdaptivePolicy,
) -> crate::AdaptiveColumnarCompactionProposal {
    let observation = database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe compaction")
        .into_iter()
        .next()
        .expect("projection observation");
    match observation
        .decide(compaction_policy, adaptive_policy)
        .expect("decide compaction")
    {
        AdaptiveColumnarCompactionDecision::Proposal(proposal) => proposal,
        AdaptiveColumnarCompactionDecision::NoAction(no_action) => {
            panic!("expected proposal, got {no_action:?}")
        }
    }
}

#[test]
fn production_eligibility_and_automatic_pressure_are_separate() {
    let mut fixture = TimelineFixture::create("phase8-eligibility-pressure");
    let no_delta = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe clean projection")
        .remove(0)
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide clean projection");
    assert!(matches!(
        no_delta,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::NoDelta
            ),
            ..
        })
    ));
    let invalid = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe for invalid policy")
        .remove(0)
        .decide(
            AdaptiveColumnarCompactionPolicy::new(0, 0),
            AdaptivePolicy::new(0, 0),
        );
    assert!(matches!(
        invalid,
        Err(crate::AdaptiveColumnarCompactionError::InvalidPolicy)
    ));

    fixture
        .database
        .execute("UPDATE events SET category = 6 WHERE id = 6")
        .expect("make projection lag");
    let lagging = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe lagging projection")
        .remove(0)
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide lagging projection");
    assert!(matches!(
        lagging,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::ProjectionLagging
            ),
            ..
        })
    ));

    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection identity");
    fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
        .expect("catch up projection");
    let observation = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe eligible compaction")
        .remove(0);
    assert!(observation.maintenance_candidate.eligible);
    assert_eq!(
        observation.maintenance_candidate.bound,
        MaintenanceBound::EstimateGatedAtomic
    );
    let below = observation
        .decide(
            AdaptiveColumnarCompactionPolicy::new(u64::MAX, u64::MAX),
            AdaptivePolicy::new(0, 0),
        )
        .expect("apply automatic pressure gate");
    assert!(matches!(
        below,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::BelowAutomaticCompactionThreshold,
            ..
        })
    ));

    let tiny = fixture
        .database
        .observe_adaptive_columnar_compactions(
            TABLE_ID,
            MaintenanceBudget::new(0, u64::MAX, u64::MAX, 1),
        )
        .expect("observe budget blocker")
        .remove(0)
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide budget blocker");
    assert!(matches!(
        tiny,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::WorkBudgetExceeded
            ),
            ..
        })
    ));

    let mut manual_compacted = false;
    for _ in 0..8 {
        let report = fixture
            .database
            .maintenance_step(generous_budget())
            .expect("manual maintenance remains independent");
        if matches!(
            report.outcome,
            MaintenanceOutcome::Completed(crate::MaintenanceActionReport::CompactColumnar(_))
        ) {
            manual_compacted = true;
            break;
        }
    }
    assert!(manual_compacted);
    fixture.close();
}

#[test]
fn snapshot_rebuild_required_and_unavailable_projections_are_blocked() {
    let mut fixture = TimelineFixture::create("phase8-ineligible-projections");
    let incremental_id = projection_id(&fixture.database);
    let snapshot_id = fixture
        .database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TABLE_ID,
            fixture.root.join("snapshot-projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build snapshot projection");

    let snapshot = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe snapshot projection")
        .into_iter()
        .find(|observation| {
            observation.maintenance_candidate.action
                == (MaintenanceAction::CompactColumnar {
                    projection_id: snapshot_id,
                })
        })
        .expect("snapshot candidate")
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide snapshot projection");
    assert!(matches!(
        snapshot,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::SnapshotProjection
            ),
            ..
        })
    ));

    fixture
        .database
        .disable_change_stream(TABLE_ID)
        .expect("invalidate incremental stream lineage");
    let rebuild = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe rebuild-required projection")
        .into_iter()
        .find(|observation| {
            observation.maintenance_candidate.action
                == (MaintenanceAction::CompactColumnar {
                    projection_id: incremental_id,
                })
        })
        .expect("incremental candidate")
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide rebuild-required projection");
    assert!(matches!(
        rebuild,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::RebuildRequired
            ),
            ..
        })
    ));

    fixture
        .database
        .projections
        .quarantine(incremental_id, "phase8 injected quarantine".into());
    let unavailable = fixture
        .database
        .observe_adaptive_columnar_compactions(TABLE_ID, generous_budget())
        .expect("observe unavailable projection")
        .into_iter()
        .find(|observation| {
            observation.maintenance_candidate.action
                == (MaintenanceAction::CompactColumnar {
                    projection_id: incremental_id,
                })
        })
        .expect("unavailable candidate")
        .decide(
            AdaptiveColumnarCompactionPolicy::new(1, 0),
            AdaptivePolicy::new(0, 0),
        )
        .expect("decide unavailable projection");
    assert!(matches!(
        unavailable,
        AdaptiveColumnarCompactionDecision::NoAction(crate::AdaptiveColumnarCompactionNoAction {
            reason: AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                MaintenanceBlocker::Unavailable
            ),
            ..
        })
    ));
    fixture.close();
}

#[test]
fn exact_revalidation_ignores_unrelated_g_but_rejects_source_and_generation_change() {
    let mut fixture = TimelineFixture::create("phase8-exact-revalidation");
    create_fresh_delta(&mut fixture.database);
    let proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, 0),
    );
    let observed_g = proposal.based_on.global_commit_seq;
    fixture
        .database
        .insert_into(CLOCK_TABLE_ID, &[ScalarValue::Int64(1)])
        .expect("advance unrelated global visibility");
    let before_execution_g = fixture
        .database
        .current_database_snapshot()
        .expect("read global snapshot")
        .expect("global visibility")
        .commit_seq();
    assert!(before_execution_g > observed_g);
    let execution = fixture
        .database
        .execute_adaptive_columnar_compaction(&proposal, generous_budget())
        .expect("unrelated G does not stale exact physical authority");
    assert_eq!(
        execution.outcome,
        AdaptiveColumnarCompactionOutcome::Completed
    );
    let measurement = execution.measurement.expect("physical measurement");
    assert_eq!(measurement.global_commit_seq_before, before_execution_g);
    assert_eq!(measurement.global_commit_seq_after, before_execution_g);
    fixture.close();

    let mut fixture = TimelineFixture::create("phase8-schema-stale");
    create_fresh_delta(&mut fixture.database);
    let proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, 0),
    );
    let generation = proposal.expected_projection_generation;
    fixture
        .database
        .execute("CREATE TABLE phase8_schema_clock (id BIGINT)")
        .expect("publish a schema change");
    let schema_stale = fixture
        .database
        .execute_adaptive_columnar_compaction(&proposal, generous_budget())
        .expect("schema change is a typed abort");
    assert_eq!(
        schema_stale.outcome,
        AdaptiveColumnarCompactionOutcome::Aborted(
            AdaptiveColumnarCompactionAbortReason::SchemaChanged
        )
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].generation,
        Some(generation)
    );
    fixture.close();

    let mut fixture = TimelineFixture::create("phase8-source-stale");
    let projection_id = create_fresh_delta(&mut fixture.database);
    let proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, 0),
    );
    let generation = fixture.database.inspect_columnar_projections()[0]
        .generation
        .expect("generation");
    fixture
        .database
        .execute("UPDATE events SET category = 5 WHERE id = 5")
        .expect("change exact source");
    let source_stale = fixture
        .database
        .execute_adaptive_columnar_compaction(&proposal, generous_budget())
        .expect("source change is a typed abort");
    assert_eq!(
        source_stale.outcome,
        AdaptiveColumnarCompactionOutcome::Aborted(
            AdaptiveColumnarCompactionAbortReason::PreconditionsChanged
        )
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].generation,
        Some(generation)
    );
    fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
        .expect("restore freshness");
    let generation_proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, 0),
    );
    fixture
        .database
        .compact_columnar_projection(projection_id)
        .expect("manual generation change");
    let current_generation = fixture.database.inspect_columnar_projections()[0]
        .generation
        .expect("new generation");
    let generation_stale = fixture
        .database
        .execute_adaptive_columnar_compaction(&generation_proposal, generous_budget())
        .expect("generation change is a typed abort");
    assert_eq!(
        generation_stale.outcome,
        AdaptiveColumnarCompactionOutcome::Aborted(
            AdaptiveColumnarCompactionAbortReason::PreconditionsChanged
        )
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].generation,
        Some(current_generation)
    );
    fixture.close();
}

#[test]
fn real_compaction_preserves_source_query_frontier_and_reports_production_measurements() {
    let mut fixture = TimelineFixture::create("phase8-real-compaction");
    let projection_id = create_fresh_delta(&mut fixture.database);
    let before_query = fixture
        .database
        .query("SELECT id, category FROM events WHERE category = 7 ORDER BY id")
        .expect("query before compaction");
    let before_source = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("source before compaction");
    let proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, 0),
    );
    let maintenance_cursor_before = fixture.database.maintenance_cursor;
    let execution = fixture
        .database
        .execute_adaptive_columnar_compaction(&proposal, generous_budget())
        .expect("execute production compaction");
    assert_eq!(
        execution.outcome,
        AdaptiveColumnarCompactionOutcome::Completed
    );
    let physical = execution.physical.as_ref().expect("production report");
    assert!(physical.compacted);
    assert_eq!(physical.projection_id, projection_id);
    assert!(physical.new_generation > physical.old_generation);
    assert!(physical.delta_segments_consumed > 0);
    assert!(physical.delta_mutations_consumed > 0);
    assert_eq!(
        physical.bytes_reclaimed,
        physical.bytes_before.saturating_sub(physical.bytes_after)
    );
    let measurement = execution
        .measurement
        .as_ref()
        .expect("adaptive measurement");
    assert!(measurement.source_snapshot_unchanged);
    assert!(measurement.source_data_version_unchanged);
    assert!(measurement.projection_frontier_preserved);
    assert_eq!(
        measurement.global_commit_seq_before,
        measurement.global_commit_seq_after
    );
    assert_eq!(
        measurement.schema_generation_before,
        measurement.schema_generation_after
    );
    let after_source = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("source after compaction");
    assert_eq!(
        before_source
            .source
            .as_ref()
            .map(|source| source.data_version),
        after_source
            .source
            .as_ref()
            .map(|source| source.data_version)
    );
    let after_query = fixture
        .database
        .query("SELECT id, category FROM events WHERE category = 7 ORDER BY id")
        .expect("query after compaction");
    assert_eq!(before_query, after_query);
    assert_eq!(
        fixture.database.maintenance_cursor, maintenance_cursor_before,
        "automatic compaction must not alter manual-maintenance rotation"
    );
    fixture.close();
}

#[test]
fn post_compaction_physical_gate_suppresses_only_the_new_generation() {
    let mut fixture = TimelineFixture::create("phase8-post-physical-gate");
    create_fresh_delta(&mut fixture.database);
    let proposal = compaction_proposal(
        &fixture.database,
        AdaptiveColumnarCompactionPolicy::new(1, 0),
        AdaptivePolicy::new(0, u64::MAX),
    );
    let old_generation = proposal.expected_projection_generation;
    let execution = fixture
        .database
        .execute_adaptive_columnar_compaction(&proposal, generous_budget())
        .expect("execute rejected physical candidate");
    assert_eq!(
        execution.outcome,
        AdaptiveColumnarCompactionOutcome::RevertedInsufficientMeasuredBenefit
    );
    let physical = execution.physical.expect("physical report");
    assert!(physical.new_generation > old_generation);
    let observed = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe suppression");
    let target = observed
        .projections
        .iter()
        .find(|target| target.projection.projection_id == Some(physical.projection_id))
        .expect("new target");
    assert!(target.suppressed_after_revert);
    assert_eq!(target.projection.generation, Some(physical.new_generation));
    assert!(
        observed
            .projections
            .iter()
            .all(|target| target.projection.generation != Some(old_generation))
    );
    assert!(matches!(
        fixture
            .database
            .inspect_maintenance(generous_budget())
            .expect("manual maintenance remains available")
            .candidates
            .iter()
            .find(|candidate| {
                candidate.action
                    == (MaintenanceAction::CompactColumnar {
                        projection_id: physical.projection_id,
                    })
            })
            .map(|candidate| candidate.blocker),
        Some(Some(MaintenanceBlocker::NoDelta))
    ));
    fixture
        .database
        .execute("UPDATE events SET category = 30 WHERE id = 3")
        .expect("create a later generation");
    fixture
        .database
        .advance_columnar_projection(
            physical.projection_id,
            ColumnarAdvanceBudget::new(16, 1 << 20),
        )
        .expect("publish a later Delta");
    fixture
        .database
        .compact_columnar_projection(physical.projection_id)
        .expect("publish later exact generation");
    let later = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe later generation");
    let later_target = later
        .projections
        .iter()
        .find(|target| target.projection.projection_id == Some(physical.projection_id))
        .expect("later target");
    assert!(later_target.projection.generation > Some(physical.new_generation));
    assert!(!later_target.suppressed_after_revert);
    fixture.close();
}
