use std::error::Error;
use std::fmt;

use netbadb_planner::{
    CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationEpoch, PlannerCalibrationProfile,
    apply_calibration_ratio,
};
use netbadb_rel::LogicalQueryShape;
use netbadb_types::SchemaGeneration;

use crate::{AdaptiveWorkloadWindow, Database};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerCalibrationPolicy {
    pub minimum_samples: u64,
    pub minimum_actual_work_units: u64,
    pub minimum_distinct_visibility_points: u64,
    pub minimum_distinct_query_shapes: u64,
    pub minimum_directional_query_shape_margin: u64,
    pub error_deadband_work_units: u64,
    pub minimum_shadow_error_improvement_work_units: u64,
    pub global_min_ratio: CalibrationRatio,
    pub global_max_ratio: CalibrationRatio,
    /// Maximum multiplicative increase from the current ratio in one epoch.
    pub maximum_step_up_ratio: CalibrationRatio,
    /// Maximum multiplicative decrease, expressed as a divisor >= 1.
    pub maximum_step_down_ratio: CalibrationRatio,
}

impl Default for PlannerCalibrationPolicy {
    fn default() -> Self {
        Self {
            minimum_samples: 8,
            minimum_actual_work_units: 1,
            minimum_distinct_visibility_points: 2,
            minimum_distinct_query_shapes: 3,
            minimum_directional_query_shape_margin: 2,
            error_deadband_work_units: 1,
            minimum_shadow_error_improvement_work_units: 1,
            global_min_ratio: CalibrationRatio::HALF,
            global_max_ratio: CalibrationRatio::DOUBLE,
            maximum_step_up_ratio: CalibrationRatio::NINE_EIGHTHS,
            maximum_step_down_ratio: CalibrationRatio::NINE_EIGHTHS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerCalibrationQueryShapeEvidence {
    pub query_shape: LogicalQueryShape,
    pub sample_count: u64,
    pub total_base_estimated_work_units: u64,
    pub total_effective_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerCalibrationEvidence {
    pub calibration_class: PlannerCalibrationClass,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub schema_generation: SchemaGeneration,
    pub sample_count: u64,
    pub total_base_estimated_work_units: u64,
    pub total_effective_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub distinct_visibility_points: u64,
    pub distinct_query_shapes: u64,
    pub underestimated_query_shapes: u64,
    pub overestimated_query_shapes: u64,
    pub within_deadband_query_shapes: u64,
    pub query_shapes: Vec<PlannerCalibrationQueryShapeEvidence>,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerCalibrationNoAction {
    InsufficientEvidence,
    IncompleteEvidence,
    InconsistentEvidence,
    WithinDeadband,
    AlreadyAtBound,
    CurrentRatioOutsideBounds,
    ArithmeticUnavailable,
    NoShadowImprovement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerCalibrationProposal {
    based_on_epoch: PlannerCalibrationEpoch,
    evidence_schema_generation: SchemaGeneration,
    calibration_class: PlannerCalibrationClass,
    current_ratio: CalibrationRatio,
    /// Aggregate actual/base ratio before either clamp.
    target_ratio: CalibrationRatio,
    /// Target after the global hard-bound clamp.
    globally_bounded_ratio: CalibrationRatio,
    /// Final target after the per-epoch step clamp.
    proposed_ratio: CalibrationRatio,
    evidence: PlannerCalibrationEvidence,
    policy: PlannerCalibrationPolicy,
}

impl PlannerCalibrationProposal {
    #[must_use]
    pub const fn based_on_epoch(&self) -> PlannerCalibrationEpoch {
        self.based_on_epoch
    }

    #[must_use]
    pub const fn evidence_schema_generation(&self) -> SchemaGeneration {
        self.evidence_schema_generation
    }

    #[must_use]
    pub const fn calibration_class(&self) -> PlannerCalibrationClass {
        self.calibration_class
    }

    #[must_use]
    pub const fn current_ratio(&self) -> CalibrationRatio {
        self.current_ratio
    }

    #[must_use]
    pub const fn target_ratio(&self) -> CalibrationRatio {
        self.target_ratio
    }

    #[must_use]
    pub const fn globally_bounded_ratio(&self) -> CalibrationRatio {
        self.globally_bounded_ratio
    }

    #[must_use]
    pub const fn proposed_ratio(&self) -> CalibrationRatio {
        self.proposed_ratio
    }

    #[must_use]
    pub const fn evidence(&self) -> &PlannerCalibrationEvidence {
        &self.evidence
    }

    #[must_use]
    pub const fn policy(&self) -> PlannerCalibrationPolicy {
        self.policy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerCalibrationDecision {
    NoAction(PlannerCalibrationNoAction),
    Proposal(Box<PlannerCalibrationProposal>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerCalibrationAdvisorError {
    SchemaChanged {
        evidence: SchemaGeneration,
        current: SchemaGeneration,
    },
    InvalidPolicy,
}

impl fmt::Display for PlannerCalibrationAdvisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaChanged { evidence, current } => write!(
                formatter,
                "planner calibration evidence schema {} differs from current schema {}",
                evidence.0, current.0
            ),
            Self::InvalidPolicy => formatter.write_str("planner calibration policy is invalid"),
        }
    }
}

impl Error for PlannerCalibrationAdvisorError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerCalibrationShadowReport {
    proposal: PlannerCalibrationProposal,
    total_old_error_work_units: u64,
    total_new_error_work_units: u64,
    improvement_work_units: u64,
    incomplete: bool,
    accepted: bool,
}

impl PlannerCalibrationShadowReport {
    #[must_use]
    pub const fn proposal(&self) -> &PlannerCalibrationProposal {
        &self.proposal
    }

    #[must_use]
    pub const fn total_old_error_work_units(&self) -> u64 {
        self.total_old_error_work_units
    }

    #[must_use]
    pub const fn total_new_error_work_units(&self) -> u64 {
        self.total_new_error_work_units
    }

    #[must_use]
    pub const fn improvement_work_units(&self) -> u64 {
        self.improvement_work_units
    }

    #[must_use]
    pub const fn incomplete(&self) -> bool {
        self.incomplete
    }

    #[must_use]
    pub const fn accepted(&self) -> bool {
        self.accepted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerCalibrationShadowDecision {
    Accepted(PlannerCalibrationShadowReport),
    Rejected {
        reason: PlannerCalibrationNoAction,
        report: PlannerCalibrationShadowReport,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerCalibrationReceipt {
    calibration_class: PlannerCalibrationClass,
    previous_epoch: PlannerCalibrationEpoch,
    applied_epoch: PlannerCalibrationEpoch,
    previous_ratio: CalibrationRatio,
    applied_ratio: CalibrationRatio,
}

impl PlannerCalibrationReceipt {
    #[must_use]
    pub const fn calibration_class(self) -> PlannerCalibrationClass {
        self.calibration_class
    }

    #[must_use]
    pub const fn previous_epoch(self) -> PlannerCalibrationEpoch {
        self.previous_epoch
    }

    #[must_use]
    pub const fn applied_epoch(self) -> PlannerCalibrationEpoch {
        self.applied_epoch
    }

    #[must_use]
    pub const fn previous_ratio(self) -> CalibrationRatio {
        self.previous_ratio
    }

    #[must_use]
    pub const fn applied_ratio(self) -> CalibrationRatio {
        self.applied_ratio
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerCalibrationMutationError {
    StaleCalibrationProposal,
    ShadowMismatch,
    ShadowRejected,
    StaleCalibrationReceipt,
    CalibrationEpochExhausted,
}

impl fmt::Display for PlannerCalibrationMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleCalibrationProposal => {
                formatter.write_str("planner calibration proposal is stale")
            }
            Self::ShadowMismatch => {
                formatter.write_str("planner calibration shadow does not match proposal")
            }
            Self::ShadowRejected => {
                formatter.write_str("planner calibration shadow was rejected or incomplete")
            }
            Self::StaleCalibrationReceipt => {
                formatter.write_str("planner calibration receipt is stale")
            }
            Self::CalibrationEpochExhausted => {
                formatter.write_str("planner calibration epoch is exhausted")
            }
        }
    }
}

impl Error for PlannerCalibrationMutationError {}

impl Database {
    #[must_use]
    pub const fn planner_calibration_profile(&self) -> PlannerCalibrationProfile {
        self.planner_calibration
    }

    pub fn advise_planner_calibration(
        &self,
        window: &AdaptiveWorkloadWindow,
        calibration_class: PlannerCalibrationClass,
        policy: PlannerCalibrationPolicy,
    ) -> Result<PlannerCalibrationDecision, PlannerCalibrationAdvisorError> {
        validate_policy(policy)?;
        let schema_generation = self.schema_generation();
        if window.target.schema_generation != schema_generation {
            return Err(PlannerCalibrationAdvisorError::SchemaChanged {
                evidence: window.target.schema_generation,
                current: schema_generation,
            });
        }
        let profile = self.planner_calibration;
        let mut evidence = aggregate_evidence(
            window,
            calibration_class,
            profile.epoch,
            policy.error_deadband_work_units,
        );
        if evidence.overflowed || evidence.incomplete || evidence.truncated {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::IncompleteEvidence,
            ));
        }
        if evidence.sample_count < policy.minimum_samples
            || evidence.total_actual_work_units < policy.minimum_actual_work_units
            || evidence.distinct_visibility_points < policy.minimum_distinct_visibility_points
            || evidence.distinct_query_shapes < policy.minimum_distinct_query_shapes
        {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::InsufficientEvidence,
            ));
        }

        let effective = evidence.total_effective_estimated_work_units;
        let actual = evidence.total_actual_work_units;
        let direction = if actual > effective {
            if actual - effective <= policy.error_deadband_work_units {
                return Ok(PlannerCalibrationDecision::NoAction(
                    PlannerCalibrationNoAction::WithinDeadband,
                ));
            }
            PlannerCalibrationDirection::Increase
        } else if effective > actual {
            if effective - actual <= policy.error_deadband_work_units {
                return Ok(PlannerCalibrationDecision::NoAction(
                    PlannerCalibrationNoAction::WithinDeadband,
                ));
            }
            PlannerCalibrationDirection::Decrease
        } else {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::WithinDeadband,
            ));
        };
        if !direction_is_consistent(direction, &evidence, policy) {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::InconsistentEvidence,
            ));
        }
        let Ok(target_ratio) = CalibrationRatio::new(
            evidence.total_actual_work_units,
            evidence.total_base_estimated_work_units,
        ) else {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::ArithmeticUnavailable,
            ));
        };
        let globally_bounded_ratio = target_ratio
            .max(policy.global_min_ratio)
            .min(policy.global_max_ratio);
        let current_ratio = profile.ratio(calibration_class);
        if current_ratio < policy.global_min_ratio || current_ratio > policy.global_max_ratio {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::CurrentRatioOutsideBounds,
            ));
        }
        let Some(step_upper) = current_ratio.checked_multiply(policy.maximum_step_up_ratio) else {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::ArithmeticUnavailable,
            ));
        };
        let Some(step_lower) = current_ratio.checked_divide(policy.maximum_step_down_ratio) else {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::ArithmeticUnavailable,
            ));
        };
        let proposed_ratio = globally_bounded_ratio.max(step_lower).min(step_upper);
        if proposed_ratio == current_ratio {
            return Ok(PlannerCalibrationDecision::NoAction(
                PlannerCalibrationNoAction::AlreadyAtBound,
            ));
        }
        // Direction counts are policy-derived diagnostics and therefore fixed
        // in the evidence embedded in this first-class proposal.
        evidence.schema_generation = schema_generation;
        Ok(PlannerCalibrationDecision::Proposal(Box::new(
            PlannerCalibrationProposal {
                based_on_epoch: profile.epoch,
                evidence_schema_generation: schema_generation,
                calibration_class,
                current_ratio,
                target_ratio,
                globally_bounded_ratio,
                proposed_ratio,
                evidence,
                policy,
            },
        )))
    }

    #[must_use]
    pub fn shadow_planner_calibration(
        &self,
        proposal: &PlannerCalibrationProposal,
    ) -> PlannerCalibrationShadowDecision {
        let mut old_error = 0_u64;
        let mut new_error = 0_u64;
        let mut incomplete = proposal.evidence.incomplete
            || proposal.evidence.overflowed
            || proposal.evidence.truncated;
        for shape in &proposal.evidence.query_shapes {
            let Some(old_effective) = apply_calibration_ratio(
                shape.total_base_estimated_work_units,
                proposal.current_ratio,
            ) else {
                incomplete = true;
                continue;
            };
            let Some(new_effective) = apply_calibration_ratio(
                shape.total_base_estimated_work_units,
                proposal.proposed_ratio,
            ) else {
                incomplete = true;
                continue;
            };
            incomplete |= checked_add(
                &mut old_error,
                absolute_difference(old_effective, shape.total_actual_work_units),
            );
            incomplete |= checked_add(
                &mut new_error,
                absolute_difference(new_effective, shape.total_actual_work_units),
            );
        }
        let improvement = old_error.saturating_sub(new_error);
        let accepted = !incomplete
            && old_error >= new_error
            && old_error - new_error >= proposal.policy.minimum_shadow_error_improvement_work_units;
        let report = PlannerCalibrationShadowReport {
            proposal: proposal.clone(),
            total_old_error_work_units: old_error,
            total_new_error_work_units: new_error,
            improvement_work_units: improvement,
            incomplete,
            accepted,
        };
        if accepted {
            PlannerCalibrationShadowDecision::Accepted(report)
        } else {
            PlannerCalibrationShadowDecision::Rejected {
                reason: if incomplete {
                    PlannerCalibrationNoAction::IncompleteEvidence
                } else {
                    PlannerCalibrationNoAction::NoShadowImprovement
                },
                report,
            }
        }
    }

    pub fn apply_planner_calibration(
        &mut self,
        proposal: &PlannerCalibrationProposal,
        shadow: &PlannerCalibrationShadowReport,
    ) -> Result<PlannerCalibrationReceipt, PlannerCalibrationMutationError> {
        if shadow.proposal != *proposal {
            return Err(PlannerCalibrationMutationError::ShadowMismatch);
        }
        let expected_shadow = self.shadow_planner_calibration(proposal);
        let PlannerCalibrationShadowDecision::Accepted(expected_shadow) = expected_shadow else {
            return Err(PlannerCalibrationMutationError::ShadowRejected);
        };
        if expected_shadow != *shadow {
            return Err(PlannerCalibrationMutationError::ShadowMismatch);
        }
        let current = self.planner_calibration;
        if self.schema_generation() != proposal.evidence_schema_generation
            || current.epoch != proposal.based_on_epoch
            || current.ratio(proposal.calibration_class) != proposal.current_ratio
        {
            return Err(PlannerCalibrationMutationError::StaleCalibrationProposal);
        }
        let next_epoch = current
            .epoch
            .checked_next()
            .ok_or(PlannerCalibrationMutationError::CalibrationEpochExhausted)?;
        self.planner_calibration = current.with_ratio(
            next_epoch,
            proposal.calibration_class,
            proposal.proposed_ratio,
        );
        Ok(PlannerCalibrationReceipt {
            calibration_class: proposal.calibration_class,
            previous_epoch: current.epoch,
            applied_epoch: next_epoch,
            previous_ratio: proposal.current_ratio,
            applied_ratio: proposal.proposed_ratio,
        })
    }

    pub fn revert_planner_calibration(
        &mut self,
        receipt: PlannerCalibrationReceipt,
    ) -> Result<PlannerCalibrationReceipt, PlannerCalibrationMutationError> {
        let current = self.planner_calibration;
        if current.epoch != receipt.applied_epoch
            || current.ratio(receipt.calibration_class) != receipt.applied_ratio
        {
            return Err(PlannerCalibrationMutationError::StaleCalibrationReceipt);
        }
        let next_epoch = current
            .epoch
            .checked_next()
            .ok_or(PlannerCalibrationMutationError::CalibrationEpochExhausted)?;
        self.planner_calibration = current.with_ratio(
            next_epoch,
            receipt.calibration_class,
            receipt.previous_ratio,
        );
        Ok(PlannerCalibrationReceipt {
            calibration_class: receipt.calibration_class,
            previous_epoch: current.epoch,
            applied_epoch: next_epoch,
            previous_ratio: receipt.applied_ratio,
            applied_ratio: receipt.previous_ratio,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannerCalibrationDirection {
    Increase,
    Decrease,
}

fn validate_policy(policy: PlannerCalibrationPolicy) -> Result<(), PlannerCalibrationAdvisorError> {
    if policy.global_min_ratio > policy.global_max_ratio
        || policy.maximum_step_up_ratio < CalibrationRatio::IDENTITY
        || policy.maximum_step_down_ratio < CalibrationRatio::IDENTITY
    {
        return Err(PlannerCalibrationAdvisorError::InvalidPolicy);
    }
    Ok(())
}

fn direction_is_consistent(
    direction: PlannerCalibrationDirection,
    evidence: &PlannerCalibrationEvidence,
    policy: PlannerCalibrationPolicy,
) -> bool {
    let (supporting, opposing) = match direction {
        PlannerCalibrationDirection::Increase => (
            evidence.underestimated_query_shapes,
            evidence.overestimated_query_shapes,
        ),
        PlannerCalibrationDirection::Decrease => (
            evidence.overestimated_query_shapes,
            evidence.underestimated_query_shapes,
        ),
    };
    supporting >= opposing && supporting - opposing >= policy.minimum_directional_query_shape_margin
}

fn aggregate_evidence(
    window: &AdaptiveWorkloadWindow,
    class: PlannerCalibrationClass,
    epoch: PlannerCalibrationEpoch,
    deadband: u64,
) -> PlannerCalibrationEvidence {
    let mut output = PlannerCalibrationEvidence {
        calibration_class: class,
        calibration_epoch: epoch,
        schema_generation: window.target.schema_generation,
        sample_count: 0,
        total_base_estimated_work_units: 0,
        total_effective_estimated_work_units: 0,
        total_actual_work_units: 0,
        distinct_visibility_points: 0,
        distinct_query_shapes: 0,
        underestimated_query_shapes: 0,
        overestimated_query_shapes: 0,
        within_deadband_query_shapes: 0,
        query_shapes: Vec::new(),
        overflowed: window.overflowed,
        incomplete: window.incomplete,
        truncated: window.truncated || window.calibration_truncated,
    };
    if let Some(visibility) = window.calibration_visibility.iter().find(|visibility| {
        visibility.calibration_class == class && visibility.calibration_epoch == epoch
    }) {
        output.distinct_visibility_points = visibility.distinct_visibility_points;
        output.overflowed |= visibility.overflowed;
    }
    for shape in &window.query_shapes {
        let mut shape_evidence = PlannerCalibrationQueryShapeEvidence {
            query_shape: shape.query_shape.clone(),
            sample_count: 0,
            total_base_estimated_work_units: 0,
            total_effective_estimated_work_units: 0,
            total_actual_work_units: 0,
            overflowed: false,
            incomplete: false,
        };
        for variant in &shape.plan_variants {
            output.truncated |= variant.calibration_truncated;
            for aggregate in &variant.calibration {
                if aggregate.calibration_class != class || aggregate.calibration_epoch != epoch {
                    continue;
                }
                shape_evidence.overflowed |= aggregate.overflowed;
                shape_evidence.incomplete |= aggregate.incomplete;
                shape_evidence.overflowed |=
                    checked_add(&mut shape_evidence.sample_count, aggregate.sample_count);
                shape_evidence.overflowed |= checked_add(
                    &mut shape_evidence.total_base_estimated_work_units,
                    aggregate.total_base_estimated_work_units,
                );
                shape_evidence.overflowed |= checked_add(
                    &mut shape_evidence.total_effective_estimated_work_units,
                    aggregate.total_effective_estimated_work_units,
                );
                shape_evidence.overflowed |= checked_add(
                    &mut shape_evidence.total_actual_work_units,
                    aggregate.total_actual_work_units,
                );
            }
        }
        if shape_evidence.sample_count == 0 && !shape_evidence.incomplete {
            continue;
        }
        output.overflowed |= shape_evidence.overflowed;
        output.incomplete |= shape_evidence.incomplete;
        output.overflowed |= checked_add(&mut output.sample_count, shape_evidence.sample_count);
        output.overflowed |= checked_add(
            &mut output.total_base_estimated_work_units,
            shape_evidence.total_base_estimated_work_units,
        );
        output.overflowed |= checked_add(
            &mut output.total_effective_estimated_work_units,
            shape_evidence.total_effective_estimated_work_units,
        );
        output.overflowed |= checked_add(
            &mut output.total_actual_work_units,
            shape_evidence.total_actual_work_units,
        );
        output.overflowed |= checked_add(&mut output.distinct_query_shapes, 1);
        let effective = shape_evidence.total_effective_estimated_work_units;
        let actual = shape_evidence.total_actual_work_units;
        if actual > effective && actual - effective > deadband {
            output.overflowed |= checked_add(&mut output.underestimated_query_shapes, 1);
        } else if effective > actual && effective - actual > deadband {
            output.overflowed |= checked_add(&mut output.overestimated_query_shapes, 1);
        } else {
            output.overflowed |= checked_add(&mut output.within_deadband_query_shapes, 1);
        }
        output.query_shapes.push(shape_evidence);
    }
    output.incomplete |= output.overflowed;
    output
}

fn absolute_difference(left: u64, right: u64) -> u64 {
    left.abs_diff(right)
}

fn checked_add(total: &mut u64, value: u64) -> bool {
    let Some(sum) = total.checked_add(value) else {
        return true;
    };
    *total = sum;
    false
}
