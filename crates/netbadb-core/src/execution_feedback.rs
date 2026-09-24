use netbadb_types::{ColumnarGeneration, ColumnarProjectionId};

use crate::{AdaptiveError, Database, DatabaseError};

/// The result of one prepared autocommit execution with explicit feedback.
///
/// Query feedback observes the same execution that produced `result`.
/// Mutations retain their ordinary result and expose a typed reason why no
/// query-work report applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedExecutionWithFeedback {
    pub result: netbadb_executor::ExecutionResult,
    pub feedback: PreparedExecutionFeedback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedExecutionFeedback {
    Query(Box<ExecutionFeedbackReport>),
    NotApplicable(PreparedFeedbackNotApplicableReason),
}

impl PreparedExecutionFeedback {
    /// Returns the query report without erasing the typed mutation case.
    #[must_use]
    pub fn query_report(&self) -> Option<&ExecutionFeedbackReport> {
        match self {
            Self::Query(report) => Some(report.as_ref()),
            Self::NotApplicable(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedFeedbackNotApplicableReason {
    Mutation,
}

pub use netbadb_query_feedback::{
    AccessExecutionFeedback, AdaptiveExecutionFeedbackOutcome, AdaptiveExecutionFeedbackReport,
    ExecutionFeedbackAnchor, ExecutionFeedbackPolicy, ExecutionFeedbackReport,
};

pub(crate) use netbadb_query_feedback::correlate_execution_feedback;

impl Database {
    /// Evaluates a slice of runtime samples for exactly one existing Columnar
    /// generation. The only mutation available to a regression outcome is
    /// runtime suppression of that derived generation.
    pub fn evaluate_adaptive_execution_feedback(
        &mut self,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
        feedback: &[ExecutionFeedbackReport],
        policy: ExecutionFeedbackPolicy,
    ) -> Result<AdaptiveExecutionFeedbackReport, AdaptiveError> {
        let projection = self
            .projections
            .iter()
            .find(|entry| entry.identity.id == projection_id)
            .and_then(|entry| entry.projection.as_ref())
            .ok_or(DatabaseError::ColumnarProjectionNotFound(projection_id))?;
        let metadata = projection.metadata();
        let table_id = metadata.table_id;
        let storage_id = metadata.source_storage_id;
        let current_snapshot = self
            .current_database_snapshot()?
            .ok_or(AdaptiveError::GlobalVisibilityRequired)?;
        let current_anchor = ExecutionFeedbackAnchor {
            global_commit_seq: Some(current_snapshot.commit_seq()),
            schema_generation: self.schema_generation(),
        };
        let report = netbadb_query_feedback::evaluate_columnar_feedback(
            netbadb_query_feedback::ColumnarFeedbackTarget {
                projection_id,
                generation,
                current_generation: metadata.generation,
                table_id,
                storage_id,
                current_anchor,
            },
            feedback,
            policy,
        );
        match report.outcome {
            AdaptiveExecutionFeedbackOutcome::RevertedMeasuredRegression => {
                self.adaptive_runtime.suppress(projection_id, generation);
            }
            AdaptiveExecutionFeedbackOutcome::ValidatedKeep => {
                self.adaptive_runtime.keep(projection_id, generation);
            }
            AdaptiveExecutionFeedbackOutcome::Inconclusive
            | AdaptiveExecutionFeedbackOutcome::StaleFeedback => {}
        }
        Ok(report)
    }
}
