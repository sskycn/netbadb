use std::error::Error;
use std::fmt;

use netbadb_executor::ExecutionAccessKind;
use netbadb_planner::PlanVariant;
use netbadb_rel::{
    BinaryOp, LogicalQueryShape, QueryColumnShape, QueryExpressionShape, QueryExpressionShapeKind,
};
use netbadb_storage::StorageKind;
use netbadb_types::{ColumnId, DatabaseCommitSeq, SchemaGeneration, TableId};

use crate::registry::TablePlacement;
use crate::{Database, DatabaseError, ExecutionFeedbackReport};

/// Fixed-width cardinality limits for one caller-owned in-memory window.
/// These are memory/evidence bounds, not recommendation thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDesignEvidenceLimits {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

impl Default for PhysicalDesignEvidenceLimits {
    fn default() -> Self {
        Self {
            max_index_candidates: 64,
            max_columnar_candidates: 64,
            max_query_shapes_per_candidate: 32,
            max_columnar_columns_per_candidate: 64,
        }
    }
}

/// Runtime-only physical-design measurement cohort identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalDesignEvidenceEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDesignEvidenceRecordOutcome {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDesignEvidenceRecordError {
    GlobalVisibilityRequired,
    StaleSchemaEvidence {
        current: SchemaGeneration,
        received: SchemaGeneration,
    },
    OutOfOrderVisibility {
        previous: DatabaseCommitSeq,
        received: DatabaseCommitSeq,
    },
    EvidenceWindowEpochExhausted,
}

impl fmt::Display for PhysicalDesignEvidenceRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalVisibilityRequired => {
                formatter.write_str("physical-design evidence requires global visibility")
            }
            Self::StaleSchemaEvidence { current, received } => write!(
                formatter,
                "physical-design schema evidence {} is older than current window schema {}",
                received.0, current.0
            ),
            Self::OutOfOrderVisibility { previous, received } => write!(
                formatter,
                "physical-design visibility {} follows newer visibility {}",
                received.0, previous.0
            ),
            Self::EvidenceWindowEpochExhausted => {
                formatter.write_str("physical-design evidence epoch is exhausted")
            }
        }
    }
}

impl Error for PhysicalDesignEvidenceRecordError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalIndexCandidate {
    pub table_id: TableId,
    pub column_id: ColumnId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalColumnarCandidate {
    pub table_id: TableId,
    /// Canonical schema declaration order in advisor reports.
    pub columns: Vec<ColumnId>,
}

/// Work paid by executions structurally relevant to a candidate. Work can
/// support several candidates and is neither additive nor predicted savings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDesignEvidenceSummary {
    pub report_count: u64,
    pub distinct_query_shapes: u64,
    pub total_actual_scan_work_units: u64,
    pub total_rows_examined: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDesignEvidenceWindowInspection {
    pub limits: PhysicalDesignEvidenceLimits,
    pub epoch: PhysicalDesignEvidenceEpoch,
    pub schema_generation: Option<SchemaGeneration>,
    pub first_global_commit_seq: Option<DatabaseCommitSeq>,
    pub last_global_commit_seq: Option<DatabaseCommitSeq>,
    pub ordering_high_water: Option<DatabaseCommitSeq>,
    pub recorded_reports: u64,
    pub index_candidate_count: u64,
    pub columnar_candidate_count: u64,
    pub capacity_rejections: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandidateEvidence {
    report_count: u64,
    query_shapes: Vec<LogicalQueryShape>,
    total_actual_scan_work_units: u64,
    total_rows_examined: u64,
    overflowed: bool,
    incomplete: bool,
    truncated: bool,
}

impl CandidateEvidence {
    const fn new() -> Self {
        Self {
            report_count: 0,
            query_shapes: Vec::new(),
            total_actual_scan_work_units: 0,
            total_rows_examined: 0,
            overflowed: false,
            incomplete: false,
            truncated: false,
        }
    }

    fn summary(&self) -> PhysicalDesignEvidenceSummary {
        PhysicalDesignEvidenceSummary {
            report_count: self.report_count,
            distinct_query_shapes: u64_len(self.query_shapes.len()),
            total_actual_scan_work_units: self.total_actual_scan_work_units,
            total_rows_examined: self.total_rows_examined,
            overflowed: self.overflowed,
            incomplete: self.incomplete,
            truncated: self.truncated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexCandidateEvidence {
    candidate: PhysicalIndexCandidate,
    point_report_count: u64,
    range_report_count: u64,
    evidence: CandidateEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnarCandidateEvidence {
    candidate: PhysicalColumnarCandidate,
    evidence: CandidateEvidence,
}

/// Caller-owned, bounded, synchronous and non-persistent design evidence.
/// Recording is explicit; ordinary query APIs never reference this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalDesignEvidenceWindow {
    limits: PhysicalDesignEvidenceLimits,
    epoch: PhysicalDesignEvidenceEpoch,
    schema_generation: Option<SchemaGeneration>,
    first_global_commit_seq: Option<DatabaseCommitSeq>,
    last_global_commit_seq: Option<DatabaseCommitSeq>,
    ordering_high_water: Option<DatabaseCommitSeq>,
    recorded_reports: u64,
    index_candidates: Vec<IndexCandidateEvidence>,
    columnar_candidates: Vec<ColumnarCandidateEvidence>,
    capacity_rejections: u64,
    discarded_incomplete_reports: u64,
    overflowed: bool,
    incomplete: bool,
    truncated: bool,
}

impl Default for PhysicalDesignEvidenceWindow {
    fn default() -> Self {
        Self::new(PhysicalDesignEvidenceLimits::default())
    }
}

impl PhysicalDesignEvidenceWindow {
    #[must_use]
    pub const fn new(limits: PhysicalDesignEvidenceLimits) -> Self {
        Self {
            limits,
            epoch: PhysicalDesignEvidenceEpoch(0),
            schema_generation: None,
            first_global_commit_seq: None,
            last_global_commit_seq: None,
            ordering_high_water: None,
            recorded_reports: 0,
            index_candidates: Vec::new(),
            columnar_candidates: Vec::new(),
            capacity_rejections: 0,
            discarded_incomplete_reports: 0,
            overflowed: false,
            incomplete: false,
            truncated: false,
        }
    }

    #[must_use]
    pub const fn limits(&self) -> PhysicalDesignEvidenceLimits {
        self.limits
    }

    #[must_use]
    pub const fn epoch(&self) -> PhysicalDesignEvidenceEpoch {
        self.epoch
    }

    #[must_use]
    pub const fn schema_generation(&self) -> Option<SchemaGeneration> {
        self.schema_generation
    }

    #[must_use]
    pub fn inspection(&self) -> PhysicalDesignEvidenceWindowInspection {
        PhysicalDesignEvidenceWindowInspection {
            limits: self.limits,
            epoch: self.epoch,
            schema_generation: self.schema_generation,
            first_global_commit_seq: self.first_global_commit_seq,
            last_global_commit_seq: self.last_global_commit_seq,
            ordering_high_water: self.ordering_high_water,
            recorded_reports: self.recorded_reports,
            index_candidate_count: u64_len(self.index_candidates.len()),
            columnar_candidate_count: u64_len(self.columnar_candidates.len()),
            capacity_rejections: self.capacity_rejections,
            discarded_incomplete_reports: self.discarded_incomplete_reports,
            overflowed: self.overflowed,
            incomplete: self.incomplete,
            truncated: self.truncated,
        }
    }

    /// Starts a new measurement cohort without allowing older G evidence to
    /// re-enter. The current schema anchor is retained.
    pub fn rotate_window(
        &mut self,
    ) -> Result<PhysicalDesignEvidenceEpoch, PhysicalDesignEvidenceRecordError> {
        let next = self
            .epoch
            .0
            .checked_add(1)
            .map(PhysicalDesignEvidenceEpoch)
            .ok_or(PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted)?;
        self.epoch = next;
        self.clear_aggregation();
        Ok(next)
    }

    pub fn record_execution_feedback(
        &mut self,
        report: &ExecutionFeedbackReport,
    ) -> Result<PhysicalDesignEvidenceRecordOutcome, PhysicalDesignEvidenceRecordError> {
        let visibility = report
            .anchor
            .global_commit_seq
            .ok_or(PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired)?;
        if let Some(previous) = self.ordering_high_water {
            if visibility < previous {
                return Err(PhysicalDesignEvidenceRecordError::OutOfOrderVisibility {
                    previous,
                    received: visibility,
                });
            }
        }

        let mut rotated = false;
        match self.schema_generation {
            None => self.schema_generation = Some(report.anchor.schema_generation),
            Some(current) if report.anchor.schema_generation < current => {
                return Err(PhysicalDesignEvidenceRecordError::StaleSchemaEvidence {
                    current,
                    received: report.anchor.schema_generation,
                });
            }
            Some(current) if report.anchor.schema_generation > current => {
                self.rotate_window()?;
                self.schema_generation = Some(report.anchor.schema_generation);
                rotated = true;
            }
            Some(_) => {}
        }

        self.ordering_high_water = Some(visibility);
        self.first_global_commit_seq.get_or_insert(visibility);
        self.last_global_commit_seq = Some(visibility);
        checked_add(&mut self.recorded_reports, 1, &mut self.overflowed);

        let rejections_before = self.capacity_rejections;
        if report.incomplete || report.overflowed {
            checked_add(
                &mut self.discarded_incomplete_reports,
                1,
                &mut self.overflowed,
            );
            self.incomplete = true;
            self.overflowed |= report.overflowed;
            return Ok(record_outcome(
                rotated,
                self.capacity_rejections != rejections_before,
            ));
        }

        let binding_counts = scan_binding_counts(&report.plan_variant);
        let scan_work = seq_scan_work(report);
        let mut index_support = Vec::new();
        collect_index_support(&report.plan_variant, &binding_counts, &mut index_support);
        deduplicate_index_support(&mut index_support);
        for support in index_support {
            let Some(work) = scan_work
                .iter()
                .find(|work| work.table_id == support.candidate.table_id)
            else {
                continue;
            };
            self.record_index_candidate(support, work, &report.query_shape);
        }

        let mut columnar_support = Vec::new();
        collect_columnar_support(&report.plan_variant, &binding_counts, &mut columnar_support);
        deduplicate_columnar_support(&mut columnar_support);
        for candidate in columnar_support {
            let Some(table_column_count) =
                logical_table_column_count(&report.query_shape, candidate.table_id)
            else {
                continue;
            };
            if u64_len(candidate.columns.len()) >= table_column_count {
                continue;
            }
            let Some(work) = scan_work
                .iter()
                .find(|work| work.table_id == candidate.table_id)
            else {
                continue;
            };
            self.record_columnar_candidate(candidate, work, &report.query_shape);
        }

        Ok(record_outcome(
            rotated,
            self.capacity_rejections != rejections_before,
        ))
    }

    fn record_index_candidate(
        &mut self,
        support: ReportIndexSupport,
        work: &ReportScanWork,
        shape: &LogicalQueryShape,
    ) {
        let position = self
            .index_candidates
            .iter()
            .position(|entry| entry.candidate == support.candidate);
        let position = match position {
            Some(position) => position,
            None => {
                if u64_len(self.index_candidates.len()) >= self.limits.max_index_candidates {
                    self.reject_capacity();
                    return;
                }
                self.index_candidates.push(IndexCandidateEvidence {
                    candidate: support.candidate,
                    point_report_count: 0,
                    range_report_count: 0,
                    evidence: CandidateEvidence::new(),
                });
                self.index_candidates.len() - 1
            }
        };
        let entry = &mut self.index_candidates[position];
        checked_add(
            &mut entry.evidence.report_count,
            1,
            &mut entry.evidence.overflowed,
        );
        if support.point {
            checked_add(
                &mut entry.point_report_count,
                1,
                &mut entry.evidence.overflowed,
            );
        }
        if support.range {
            checked_add(
                &mut entry.range_report_count,
                1,
                &mut entry.evidence.overflowed,
            );
        }
        let capacity_rejected = record_candidate_work(
            &mut entry.evidence,
            work,
            shape,
            self.limits.max_query_shapes_per_candidate,
        );
        self.observe_candidate_health(position, true);
        if capacity_rejected {
            self.reject_capacity();
        }
    }

    fn record_columnar_candidate(
        &mut self,
        candidate: PhysicalColumnarCandidate,
        work: &ReportScanWork,
        shape: &LogicalQueryShape,
    ) {
        if u64_len(candidate.columns.len()) > self.limits.max_columnar_columns_per_candidate {
            self.reject_capacity();
            return;
        }
        let position = self.columnar_candidates.iter().position(|entry| {
            entry.candidate.table_id == candidate.table_id
                && same_column_set(&entry.candidate.columns, &candidate.columns)
        });
        let position = match position {
            Some(position) => position,
            None => {
                if u64_len(self.columnar_candidates.len()) >= self.limits.max_columnar_candidates {
                    self.reject_capacity();
                    return;
                }
                self.columnar_candidates.push(ColumnarCandidateEvidence {
                    candidate,
                    evidence: CandidateEvidence::new(),
                });
                self.columnar_candidates.len() - 1
            }
        };
        let entry = &mut self.columnar_candidates[position];
        checked_add(
            &mut entry.evidence.report_count,
            1,
            &mut entry.evidence.overflowed,
        );
        let capacity_rejected = record_candidate_work(
            &mut entry.evidence,
            work,
            shape,
            self.limits.max_query_shapes_per_candidate,
        );
        self.observe_candidate_health(position, false);
        if capacity_rejected {
            self.reject_capacity();
        }
    }

    fn reject_capacity(&mut self) {
        checked_add(&mut self.capacity_rejections, 1, &mut self.overflowed);
        self.truncated = true;
    }

    fn observe_candidate_health(&mut self, position: usize, index: bool) {
        let evidence = if index {
            &self.index_candidates[position].evidence
        } else {
            &self.columnar_candidates[position].evidence
        };
        self.overflowed |= evidence.overflowed;
        self.incomplete |= evidence.incomplete;
    }

    fn clear_aggregation(&mut self) {
        self.first_global_commit_seq = None;
        self.last_global_commit_seq = None;
        self.recorded_reports = 0;
        self.index_candidates.clear();
        self.columnar_candidates.clear();
        self.capacity_rejections = 0;
        self.discarded_incomplete_reports = 0;
        self.overflowed = false;
        self.incomplete = false;
        self.truncated = false;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDesignRecommendationPolicy {
    pub minimum_reports: u64,
    pub minimum_distinct_query_shapes: u64,
    pub minimum_actual_scan_work_units: u64,
    pub max_recommendations: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDesignAdvisorPolicy {
    pub index: PhysicalDesignRecommendationPolicy,
    pub columnar: PhysicalDesignRecommendationPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDesignCandidateDecision {
    Recommend,
    NoAction(PhysicalDesignNoActionReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDesignNoActionReason {
    BelowMinimumReports,
    BelowMinimumShapeDiversity,
    BelowMinimumActualWork,
    ExistingDesignCovers,
    UnsupportedCurrentLayout,
    IncompleteEvidence,
    CurrentProjectionUnavailable,
    RecommendationLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalIndexRecommendationInspection {
    pub candidate: PhysicalIndexCandidate,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: PhysicalDesignEvidenceSummary,
    pub decision: PhysicalDesignCandidateDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalColumnarRecommendationInspection {
    pub candidate: PhysicalColumnarCandidate,
    pub evidence: PhysicalDesignEvidenceSummary,
    pub decision: PhysicalDesignCandidateDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalDesignAdvisorReport {
    pub evidence_epoch: PhysicalDesignEvidenceEpoch,
    pub schema_generation: SchemaGeneration,
    pub first_global_commit_seq: DatabaseCommitSeq,
    pub last_global_commit_seq: DatabaseCommitSeq,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<PhysicalIndexRecommendationInspection>,
    pub columnar_candidates: Vec<PhysicalColumnarRecommendationInspection>,
}

#[derive(Debug)]
pub enum PhysicalDesignAdvisorError {
    NoEvidence,
    StaleSchema {
        evidence: SchemaGeneration,
        current: SchemaGeneration,
    },
    InconclusiveCapacity {
        capacity_rejections: u64,
    },
    Database(DatabaseError),
}

impl fmt::Display for PhysicalDesignAdvisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEvidence => formatter.write_str("physical-design evidence window is empty"),
            Self::StaleSchema { evidence, current } => write!(
                formatter,
                "physical-design evidence schema {} does not match current schema {}",
                evidence.0, current.0
            ),
            Self::InconclusiveCapacity {
                capacity_rejections,
            } => write!(
                formatter,
                "physical-design evidence is capacity-truncated ({capacity_rejections} rejections)"
            ),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl Error for PhysicalDesignAdvisorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DatabaseError> for PhysicalDesignAdvisorError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl Database {
    /// Inspects evidence against current production inventory and capability.
    /// This method reserves no identity and mutates neither database nor input.
    pub fn advise_physical_design(
        &self,
        evidence: &PhysicalDesignEvidenceWindow,
        policy: PhysicalDesignAdvisorPolicy,
    ) -> Result<PhysicalDesignAdvisorReport, PhysicalDesignAdvisorError> {
        if evidence.recorded_reports == 0 {
            return Err(PhysicalDesignAdvisorError::NoEvidence);
        }
        let evidence_schema = evidence
            .schema_generation
            .ok_or(PhysicalDesignAdvisorError::NoEvidence)?;
        let current_schema = self.schema_generation();
        if evidence_schema != current_schema {
            return Err(PhysicalDesignAdvisorError::StaleSchema {
                evidence: evidence_schema,
                current: current_schema,
            });
        }
        if evidence.truncated {
            return Err(PhysicalDesignAdvisorError::InconclusiveCapacity {
                capacity_rejections: evidence.capacity_rejections,
            });
        }
        self.ensure_schema_available(None)?;

        let mut index_candidates = evidence
            .index_candidates
            .iter()
            .map(|entry| self.inspect_index_candidate(entry, policy.index))
            .collect::<Result<Vec<_>, PhysicalDesignAdvisorError>>()?;
        rank_index_inspections(&mut index_candidates);
        enforce_recommendation_limit(&mut index_candidates, policy.index.max_recommendations);

        let mut columnar_candidates = evidence
            .columnar_candidates
            .iter()
            .map(|entry| self.inspect_columnar_candidate(entry, policy.columnar))
            .collect::<Result<Vec<_>, PhysicalDesignAdvisorError>>()?;
        rank_columnar_inspections(&mut columnar_candidates);
        enforce_columnar_recommendation_limit(
            &mut columnar_candidates,
            policy.columnar.max_recommendations,
        );

        Ok(PhysicalDesignAdvisorReport {
            evidence_epoch: evidence.epoch,
            schema_generation: evidence_schema,
            first_global_commit_seq: evidence
                .first_global_commit_seq
                .ok_or(PhysicalDesignAdvisorError::NoEvidence)?,
            last_global_commit_seq: evidence
                .last_global_commit_seq
                .ok_or(PhysicalDesignAdvisorError::NoEvidence)?,
            recorded_reports: evidence.recorded_reports,
            discarded_incomplete_reports: evidence.discarded_incomplete_reports,
            overflowed: evidence.overflowed,
            incomplete: evidence.incomplete,
            index_candidates,
            columnar_candidates,
        })
    }

    fn inspect_index_candidate(
        &self,
        entry: &IndexCandidateEvidence,
        policy: PhysicalDesignRecommendationPolicy,
    ) -> Result<PhysicalIndexRecommendationInspection, PhysicalDesignAdvisorError> {
        let summary = entry.evidence.summary();
        let decision = if summary.overflowed || summary.incomplete || summary.truncated {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::IncompleteEvidence,
            )
        } else if self.current_access_path_covers(entry)? {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::ExistingDesignCovers,
            )
        } else if !self.index_candidate_is_production_actionable(entry.candidate)? {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
            )
        } else {
            threshold_decision(summary, policy)
        };
        Ok(PhysicalIndexRecommendationInspection {
            candidate: entry.candidate,
            point_report_count: entry.point_report_count,
            range_report_count: entry.range_report_count,
            evidence: summary,
            decision,
        })
    }

    fn current_access_path_covers(
        &self,
        entry: &IndexCandidateEvidence,
    ) -> Result<bool, PhysicalDesignAdvisorError> {
        let storage_id = match self
            .bindings
            .placement(entry.candidate.table_id)
            .map_err(DatabaseError::from)?
        {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => return Ok(false),
        };
        let storage = self.registry.get(storage_id).ok_or({
            DatabaseError::InspectionStorageMissing {
                table_id: entry.candidate.table_id,
            }
        })?;
        Ok(storage.access_paths().into_iter().any(|path| {
            path.column_id == entry.candidate.column_id
                && (entry.point_report_count == 0 || path.capabilities.point_lookup)
                && (entry.range_report_count == 0 || path.capabilities.range_lookup)
        }))
    }

    fn index_candidate_is_production_actionable(
        &self,
        candidate: PhysicalIndexCandidate,
    ) -> Result<bool, PhysicalDesignAdvisorError> {
        let Some(table) = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == candidate.table_id)
        else {
            return Ok(false);
        };
        if table.column_by_id(candidate.column_id).is_none() {
            return Ok(false);
        }
        let storage_id = match self
            .bindings
            .placement(candidate.table_id)
            .map_err(DatabaseError::from)?
        {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => return Ok(false),
        };
        let storage = self.registry.get(storage_id).ok_or({
            DatabaseError::InspectionStorageMissing {
                table_id: candidate.table_id,
            }
        })?;
        Ok(storage.kind() == StorageKind::Heap)
    }

    fn inspect_columnar_candidate(
        &self,
        entry: &ColumnarCandidateEvidence,
        policy: PhysicalDesignRecommendationPolicy,
    ) -> Result<PhysicalColumnarRecommendationInspection, PhysicalDesignAdvisorError> {
        let summary = entry.evidence.summary();
        let mut candidate = entry.candidate.clone();
        let Some(table) = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == candidate.table_id)
        else {
            return Ok(PhysicalColumnarRecommendationInspection {
                candidate,
                evidence: summary,
                decision: PhysicalDesignCandidateDecision::NoAction(
                    PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
                ),
            });
        };
        candidate.columns = table
            .columns
            .iter()
            .filter(|column| candidate.columns.contains(&column.id))
            .map(|column| column.id)
            .collect();
        let storage_id = match self
            .bindings
            .placement(candidate.table_id)
            .map_err(DatabaseError::from)?
        {
            TablePlacement::Single { storage_id, .. } => Some(*storage_id),
            TablePlacement::RangePartitioned { .. } => None,
        };
        if let Some(storage_id) = storage_id {
            self.registry.get(storage_id).ok_or({
                DatabaseError::InspectionStorageMissing {
                    table_id: candidate.table_id,
                }
            })?;
        }

        let decision = if summary.overflowed || summary.incomplete || summary.truncated {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::IncompleteEvidence,
            )
        } else if candidate.columns.is_empty()
            || candidate.columns.len() >= table.columns.len()
            || candidate.columns.len() != entry.candidate.columns.len()
            || storage_id.is_none()
        {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
            )
        } else if !self.projections.catalog_inspection().available
            || self.projections.iter().any(|projection| {
                projection.identity.table_id == candidate.table_id
                    && projection.projection.is_none()
            })
        {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::CurrentProjectionUnavailable,
            )
        } else if self.projections.iter().any(|entry| {
            entry.projection.as_ref().is_some_and(|projection| {
                let metadata = projection.metadata();
                metadata.table_id == candidate.table_id
                    && candidate.columns.iter().all(|column| {
                        metadata
                            .columns
                            .iter()
                            .any(|projected| projected.column_id == *column)
                    })
            })
        }) {
            PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::ExistingDesignCovers,
            )
        } else {
            threshold_decision(summary, policy)
        };
        Ok(PhysicalColumnarRecommendationInspection {
            candidate,
            evidence: summary,
            decision,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReportIndexSupport {
    candidate: PhysicalIndexCandidate,
    point: bool,
    range: bool,
}

#[derive(Debug, Clone, Copy)]
struct ReportScanWork {
    table_id: TableId,
    actual_work_units: u64,
    rows_examined: u64,
    overflowed: bool,
    incomplete: bool,
}

fn seq_scan_work(report: &ExecutionFeedbackReport) -> Vec<ReportScanWork> {
    let mut result = Vec::new();
    for access in &report.accesses {
        if access.actual.kind != ExecutionAccessKind::SeqScan {
            continue;
        }
        let position = result
            .iter()
            .position(|entry: &ReportScanWork| entry.table_id == access.actual.table_id);
        let position = position.unwrap_or_else(|| {
            result.push(ReportScanWork {
                table_id: access.actual.table_id,
                actual_work_units: 0,
                rows_examined: 0,
                overflowed: false,
                incomplete: false,
            });
            result.len() - 1
        });
        let work = &mut result[position];
        work.overflowed |= access.actual.work.overflowed;
        work.incomplete |= access.actual.work.incomplete;
        checked_add(
            &mut work.rows_examined,
            access.actual.work.rows_examined,
            &mut work.overflowed,
        );
        match access
            .calibration
            .as_ref()
            .and_then(|sample| sample.actual_work_units)
        {
            Some(actual) => checked_add(&mut work.actual_work_units, actual, &mut work.overflowed),
            None => work.incomplete = true,
        }
    }
    result
}

fn scan_binding_counts(plan: &PlanVariant) -> Vec<(TableId, u64)> {
    let mut bindings = Vec::new();
    collect_scan_bindings(plan, &mut bindings);
    let mut counts = Vec::new();
    for (table_id, binding) in bindings {
        if counts.iter().any(|(seen_table, seen_binding, _)| {
            *seen_table == table_id && *seen_binding == binding
        }) {
            continue;
        }
        if let Some((_, _, count)) = counts
            .iter_mut()
            .find(|(seen_table, _, _)| *seen_table == table_id)
        {
            *count += 1;
        } else {
            counts.push((table_id, binding, 1_u64));
        }
    }
    counts
        .into_iter()
        .map(|(table_id, _, count)| (table_id, count))
        .collect()
}

fn collect_scan_bindings(
    plan: &PlanVariant,
    output: &mut Vec<(TableId, netbadb_rel::CanonicalBindingOrdinal)>,
) {
    match plan {
        PlanVariant::SeqScan {
            binding, table_id, ..
        }
        | PlanVariant::ColumnarScan {
            binding, table_id, ..
        }
        | PlanVariant::IndexPoint {
            binding, table_id, ..
        }
        | PlanVariant::IndexRange {
            binding, table_id, ..
        }
        | PlanVariant::PartitionedAccess {
            binding, table_id, ..
        } => output.push((*table_id, *binding)),
        PlanVariant::NestedLoopJoin { left, right, .. }
        | PlanVariant::HashJoin { left, right, .. } => {
            collect_scan_bindings(left, output);
            collect_scan_bindings(right, output);
        }
        PlanVariant::IndexNestedLoopJoin {
            left,
            right_binding,
            right_table_id,
            ..
        } => {
            collect_scan_bindings(left, output);
            output.push((*right_table_id, *right_binding));
        }
        PlanVariant::Filter { input, .. }
        | PlanVariant::Sort { input, .. }
        | PlanVariant::Project { input, .. }
        | PlanVariant::ScalarProject { input, .. }
        | PlanVariant::Aggregate { input, .. }
        | PlanVariant::Limit { input, .. } => collect_scan_bindings(input, output),
        PlanVariant::OneRow => {}
    }
}

fn collect_index_support(
    plan: &PlanVariant,
    binding_counts: &[(TableId, u64)],
    output: &mut Vec<ReportIndexSupport>,
) {
    match plan {
        PlanVariant::Filter { input, predicate } => {
            if let PlanVariant::SeqScan {
                binding, table_id, ..
            } = input.as_ref()
            {
                if unique_table(*table_id, binding_counts) {
                    collect_indexable_comparisons(predicate, *binding, *table_id, output);
                }
            }
            collect_index_support(input, binding_counts, output);
        }
        PlanVariant::NestedLoopJoin { left, right, .. }
        | PlanVariant::HashJoin { left, right, .. } => {
            collect_index_support(left, binding_counts, output);
            collect_index_support(right, binding_counts, output);
        }
        PlanVariant::IndexNestedLoopJoin { left, .. } => {
            collect_index_support(left, binding_counts, output);
        }
        PlanVariant::Sort { input, .. }
        | PlanVariant::Project { input, .. }
        | PlanVariant::ScalarProject { input, .. }
        | PlanVariant::Aggregate { input, .. }
        | PlanVariant::Limit { input, .. } => collect_index_support(input, binding_counts, output),
        PlanVariant::OneRow
        | PlanVariant::SeqScan { .. }
        | PlanVariant::ColumnarScan { .. }
        | PlanVariant::IndexPoint { .. }
        | PlanVariant::IndexRange { .. }
        | PlanVariant::PartitionedAccess { .. } => {}
    }
}

fn collect_indexable_comparisons(
    predicate: &QueryExpressionShape,
    binding: netbadb_rel::CanonicalBindingOrdinal,
    table_id: TableId,
    output: &mut Vec<ReportIndexSupport>,
) {
    let QueryExpressionShapeKind::Binary {
        operator,
        left,
        right,
    } = &predicate.kind
    else {
        return;
    };
    if *operator == BinaryOp::And {
        collect_indexable_comparisons(left, binding, table_id, output);
        collect_indexable_comparisons(right, binding, table_id, output);
        return;
    }
    let (point, range) = match operator {
        BinaryOp::Eq => (true, false),
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => (false, true),
        BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => return,
    };
    let column =
        direct_column_and_value(left, right).or_else(|| direct_column_and_value(right, left));
    let Some(column) = column else {
        return;
    };
    if column.binding != binding || column.table_id != table_id {
        return;
    }
    output.push(ReportIndexSupport {
        candidate: PhysicalIndexCandidate {
            table_id,
            column_id: column.column_id,
        },
        point,
        range,
    });
}

fn direct_column_and_value<'a>(
    column: &'a QueryExpressionShape,
    value: &QueryExpressionShape,
) -> Option<&'a QueryColumnShape> {
    let QueryExpressionShapeKind::Column(column) = &column.kind else {
        return None;
    };
    match &value.kind {
        QueryExpressionShapeKind::Literal(literal) if !literal.is_null => Some(column),
        QueryExpressionShapeKind::Parameter(_) => Some(column),
        _ => None,
    }
}

fn collect_columnar_support(
    plan: &PlanVariant,
    binding_counts: &[(TableId, u64)],
    output: &mut Vec<PhysicalColumnarCandidate>,
) {
    match plan {
        PlanVariant::SeqScan {
            table_id, columns, ..
        } if unique_table(*table_id, binding_counts) => {
            let mut column_ids = Vec::new();
            for column in columns {
                if !column_ids.contains(&column.column_id) {
                    column_ids.push(column.column_id);
                }
            }
            if !column_ids.is_empty() {
                output.push(PhysicalColumnarCandidate {
                    table_id: *table_id,
                    columns: column_ids,
                });
            }
        }
        PlanVariant::NestedLoopJoin { left, right, .. }
        | PlanVariant::HashJoin { left, right, .. } => {
            collect_columnar_support(left, binding_counts, output);
            collect_columnar_support(right, binding_counts, output);
        }
        PlanVariant::IndexNestedLoopJoin { left, .. } => {
            collect_columnar_support(left, binding_counts, output);
        }
        PlanVariant::Filter { input, .. }
        | PlanVariant::Sort { input, .. }
        | PlanVariant::Project { input, .. }
        | PlanVariant::ScalarProject { input, .. }
        | PlanVariant::Aggregate { input, .. }
        | PlanVariant::Limit { input, .. } => {
            collect_columnar_support(input, binding_counts, output);
        }
        PlanVariant::OneRow
        | PlanVariant::SeqScan { .. }
        | PlanVariant::ColumnarScan { .. }
        | PlanVariant::IndexPoint { .. }
        | PlanVariant::IndexRange { .. }
        | PlanVariant::PartitionedAccess { .. } => {}
    }
}

fn unique_table(table_id: TableId, counts: &[(TableId, u64)]) -> bool {
    counts
        .iter()
        .find(|(candidate, _)| *candidate == table_id)
        .is_some_and(|(_, count)| *count == 1)
}

fn deduplicate_index_support(support: &mut Vec<ReportIndexSupport>) {
    let mut deduplicated: Vec<ReportIndexSupport> = Vec::new();
    for item in support.drain(..) {
        if let Some(existing) = deduplicated
            .iter_mut()
            .find(|existing| existing.candidate == item.candidate)
        {
            existing.point |= item.point;
            existing.range |= item.range;
        } else {
            deduplicated.push(item);
        }
    }
    *support = deduplicated;
}

fn deduplicate_columnar_support(support: &mut Vec<PhysicalColumnarCandidate>) {
    let mut deduplicated: Vec<PhysicalColumnarCandidate> = Vec::new();
    for item in support.drain(..) {
        if !deduplicated.iter().any(|candidate| {
            candidate.table_id == item.table_id
                && same_column_set(&candidate.columns, &item.columns)
        }) {
            deduplicated.push(item);
        }
    }
    *support = deduplicated;
}

fn same_column_set(left: &[ColumnId], right: &[ColumnId]) -> bool {
    left.len() == right.len() && left.iter().all(|column| right.contains(column))
}

fn logical_table_column_count(shape: &LogicalQueryShape, table_id: TableId) -> Option<u64> {
    match shape {
        LogicalQueryShape::Scan {
            table_id: scanned,
            columns,
            ..
        } => (*scanned == table_id).then(|| {
            let mut unique = Vec::new();
            for column in columns {
                if !unique.contains(&column.column_id) {
                    unique.push(column.column_id);
                }
            }
            u64_len(unique.len())
        }),
        LogicalQueryShape::Join { left, right, .. } => logical_table_column_count(left, table_id)
            .or_else(|| logical_table_column_count(right, table_id)),
        LogicalQueryShape::Filter { input, .. }
        | LogicalQueryShape::Sort { input, .. }
        | LogicalQueryShape::Project { input, .. }
        | LogicalQueryShape::ScalarProject { input, .. }
        | LogicalQueryShape::Aggregate { input, .. }
        | LogicalQueryShape::Limit { input, .. } => logical_table_column_count(input, table_id),
        LogicalQueryShape::OneRow => None,
    }
}

fn record_candidate_work(
    evidence: &mut CandidateEvidence,
    work: &ReportScanWork,
    shape: &LogicalQueryShape,
    maximum_shapes: u64,
) -> bool {
    checked_add(
        &mut evidence.total_actual_scan_work_units,
        work.actual_work_units,
        &mut evidence.overflowed,
    );
    checked_add(
        &mut evidence.total_rows_examined,
        work.rows_examined,
        &mut evidence.overflowed,
    );
    evidence.overflowed |= work.overflowed;
    evidence.incomplete |= work.incomplete;
    if !evidence.query_shapes.contains(shape) {
        if u64_len(evidence.query_shapes.len()) >= maximum_shapes {
            evidence.truncated = true;
            return true;
        } else {
            evidence.query_shapes.push(shape.clone());
        }
    }
    false
}

fn threshold_decision(
    evidence: PhysicalDesignEvidenceSummary,
    policy: PhysicalDesignRecommendationPolicy,
) -> PhysicalDesignCandidateDecision {
    let reason = if evidence.report_count < policy.minimum_reports {
        Some(PhysicalDesignNoActionReason::BelowMinimumReports)
    } else if evidence.distinct_query_shapes < policy.minimum_distinct_query_shapes {
        Some(PhysicalDesignNoActionReason::BelowMinimumShapeDiversity)
    } else if evidence.total_actual_scan_work_units < policy.minimum_actual_scan_work_units {
        Some(PhysicalDesignNoActionReason::BelowMinimumActualWork)
    } else {
        None
    };
    reason.map_or(PhysicalDesignCandidateDecision::Recommend, |reason| {
        PhysicalDesignCandidateDecision::NoAction(reason)
    })
}

fn rank_index_inspections(inspections: &mut [PhysicalIndexRecommendationInspection]) {
    inspections.sort_by(|left, right| {
        right
            .evidence
            .total_actual_scan_work_units
            .cmp(&left.evidence.total_actual_scan_work_units)
            .then_with(|| right.evidence.report_count.cmp(&left.evidence.report_count))
            .then_with(|| {
                right
                    .evidence
                    .distinct_query_shapes
                    .cmp(&left.evidence.distinct_query_shapes)
            })
            .then_with(|| left.candidate.table_id.cmp(&right.candidate.table_id))
            .then_with(|| left.candidate.column_id.cmp(&right.candidate.column_id))
    });
}

fn rank_columnar_inspections(inspections: &mut [PhysicalColumnarRecommendationInspection]) {
    inspections.sort_by(|left, right| {
        right
            .evidence
            .total_actual_scan_work_units
            .cmp(&left.evidence.total_actual_scan_work_units)
            .then_with(|| right.evidence.report_count.cmp(&left.evidence.report_count))
            .then_with(|| {
                right
                    .evidence
                    .distinct_query_shapes
                    .cmp(&left.evidence.distinct_query_shapes)
            })
            .then_with(|| left.candidate.table_id.cmp(&right.candidate.table_id))
            .then_with(|| left.candidate.columns.cmp(&right.candidate.columns))
    });
}

fn enforce_recommendation_limit(
    inspections: &mut [PhysicalIndexRecommendationInspection],
    maximum: u32,
) {
    let mut retained = 0_u32;
    for inspection in inspections {
        if inspection.decision != PhysicalDesignCandidateDecision::Recommend {
            continue;
        }
        if retained < maximum {
            retained += 1;
        } else {
            inspection.decision = PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::RecommendationLimitReached,
            );
        }
    }
}

fn enforce_columnar_recommendation_limit(
    inspections: &mut [PhysicalColumnarRecommendationInspection],
    maximum: u32,
) {
    let mut retained = 0_u32;
    for inspection in inspections {
        if inspection.decision != PhysicalDesignCandidateDecision::Recommend {
            continue;
        }
        if retained < maximum {
            retained += 1;
        } else {
            inspection.decision = PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::RecommendationLimitReached,
            );
        }
    }
}

fn record_outcome(rotated: bool, capacity_rejected: bool) -> PhysicalDesignEvidenceRecordOutcome {
    match (rotated, capacity_rejected) {
        (false, false) => PhysicalDesignEvidenceRecordOutcome::Recorded,
        (true, false) => PhysicalDesignEvidenceRecordOutcome::SchemaRotated,
        (false, true) => PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection,
        (true, true) => PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection,
    }
}

fn checked_add(target: &mut u64, value: u64, overflowed: &mut bool) {
    match target.checked_add(value) {
        Some(sum) => *target = sum,
        None => {
            *target = u64::MAX;
            *overflowed = true;
        }
    }
}

fn u64_len(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}
