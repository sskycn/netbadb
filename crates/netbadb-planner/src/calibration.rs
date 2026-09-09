use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

/// Runtime-only generation of one immutable planner calibration snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlannerCalibrationEpoch(pub u64);

impl PlannerCalibrationEpoch {
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// The only access classes supported by the first global calibration overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PlannerCalibrationClass {
    SeqScan,
    IndexPoint,
    IndexRange,
    Columnar,
}

/// A normalized, positive integer ratio used for deterministic cost scaling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CalibrationRatio {
    numerator: u64,
    denominator: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationRatioError {
    ZeroNumerator,
    ZeroDenominator,
}

impl fmt::Display for CalibrationRatioError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroNumerator => {
                formatter.write_str("calibration ratio numerator must be nonzero")
            }
            Self::ZeroDenominator => {
                formatter.write_str("calibration ratio denominator must be nonzero")
            }
        }
    }
}

impl Error for CalibrationRatioError {}

impl CalibrationRatio {
    pub const IDENTITY: Self = Self {
        numerator: 1,
        denominator: 1,
    };
    pub const HALF: Self = Self {
        numerator: 1,
        denominator: 2,
    };
    pub const DOUBLE: Self = Self {
        numerator: 2,
        denominator: 1,
    };
    pub const NINE_EIGHTHS: Self = Self {
        numerator: 9,
        denominator: 8,
    };

    pub fn new(numerator: u64, denominator: u64) -> Result<Self, CalibrationRatioError> {
        if numerator == 0 {
            return Err(CalibrationRatioError::ZeroNumerator);
        }
        if denominator == 0 {
            return Err(CalibrationRatioError::ZeroDenominator);
        }
        let divisor = greatest_common_divisor(numerator, denominator);
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    #[must_use]
    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    #[must_use]
    pub const fn denominator(self) -> u64 {
        self.denominator
    }

    #[must_use]
    pub fn checked_multiply(self, other: Self) -> Option<Self> {
        let numerator = u128::from(self.numerator).checked_mul(u128::from(other.numerator))?;
        let denominator =
            u128::from(self.denominator).checked_mul(u128::from(other.denominator))?;
        normalized_u128_ratio(numerator, denominator)
    }

    #[must_use]
    pub fn checked_divide(self, other: Self) -> Option<Self> {
        let numerator = u128::from(self.numerator).checked_mul(u128::from(other.denominator))?;
        let denominator = u128::from(self.denominator).checked_mul(u128::from(other.numerator))?;
        normalized_u128_ratio(numerator, denominator)
    }
}

impl Default for CalibrationRatio {
    fn default() -> Self {
        Self::IDENTITY
    }
}

impl fmt::Display for CalibrationRatio {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.numerator, self.denominator)
    }
}

impl PartialOrd for CalibrationRatio {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CalibrationRatio {
    fn cmp(&self, other: &Self) -> Ordering {
        (u128::from(self.numerator) * u128::from(other.denominator))
            .cmp(&(u128::from(other.numerator) * u128::from(self.denominator)))
    }
}

/// Applies one ratio with ceiling rounding. Zero remains zero. An unavailable
/// result is telemetry/model overflow and callers must fall back to base work.
#[must_use]
pub fn apply_calibration_ratio(base_work_units: u64, ratio: CalibrationRatio) -> Option<u64> {
    let scaled = apply_ratio_u128(u128::from(base_work_units), ratio)?;
    u64::try_from(scaled).ok()
}

pub(crate) fn effective_planning_cost(base_work_units: u128, ratio: CalibrationRatio) -> u128 {
    apply_ratio_u128(base_work_units, ratio).unwrap_or(base_work_units)
}

fn apply_ratio_u128(base_work_units: u128, ratio: CalibrationRatio) -> Option<u128> {
    if base_work_units == 0 {
        return Some(0);
    }
    let product = base_work_units.checked_mul(u128::from(ratio.numerator))?;
    let denominator = u128::from(ratio.denominator);
    let quotient = product / denominator;
    let rounded = u128::from(product % denominator != 0);
    quotient.checked_add(rounded)
}

fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn normalized_u128_ratio(numerator: u128, denominator: u128) -> Option<CalibrationRatio> {
    if numerator == 0 || denominator == 0 {
        return None;
    }
    let mut left = numerator;
    let mut right = denominator;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    CalibrationRatio::new(
        u64::try_from(numerator / left).ok()?,
        u64::try_from(denominator / left).ok()?,
    )
    .ok()
}

/// Fixed global access-class overlay supplied immutably to one planning call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlannerCalibrationProfile {
    pub epoch: PlannerCalibrationEpoch,
    pub seq_scan: CalibrationRatio,
    pub index_point: CalibrationRatio,
    pub index_range: CalibrationRatio,
    pub columnar: CalibrationRatio,
}

impl PlannerCalibrationProfile {
    pub const IDENTITY: Self = Self {
        epoch: PlannerCalibrationEpoch(0),
        seq_scan: CalibrationRatio::IDENTITY,
        index_point: CalibrationRatio::IDENTITY,
        index_range: CalibrationRatio::IDENTITY,
        columnar: CalibrationRatio::IDENTITY,
    };

    #[must_use]
    pub const fn ratio(self, class: PlannerCalibrationClass) -> CalibrationRatio {
        match class {
            PlannerCalibrationClass::SeqScan => self.seq_scan,
            PlannerCalibrationClass::IndexPoint => self.index_point,
            PlannerCalibrationClass::IndexRange => self.index_range,
            PlannerCalibrationClass::Columnar => self.columnar,
        }
    }

    #[must_use]
    pub const fn with_ratio(
        mut self,
        epoch: PlannerCalibrationEpoch,
        class: PlannerCalibrationClass,
        ratio: CalibrationRatio,
    ) -> Self {
        self.epoch = epoch;
        match class {
            PlannerCalibrationClass::SeqScan => self.seq_scan = ratio,
            PlannerCalibrationClass::IndexPoint => self.index_point = ratio,
            PlannerCalibrationClass::IndexRange => self.index_range = ratio,
            PlannerCalibrationClass::Columnar => self.columnar = ratio,
        }
        self
    }
}

impl Default for PlannerCalibrationProfile {
    fn default() -> Self {
        Self::IDENTITY
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationProfile,
        apply_calibration_ratio,
    };

    #[test]
    fn ratios_normalize_order_and_apply_with_ceiling_rounding() {
        let ratio = CalibrationRatio::new(6, 4).expect("ratio");
        assert_eq!(ratio, CalibrationRatio::new(3, 2).expect("ratio"));
        assert_eq!(ratio.to_string(), "3/2");
        assert_eq!(apply_calibration_ratio(0, ratio), Some(0));
        assert_eq!(apply_calibration_ratio(1, ratio), Some(2));
        assert_eq!(apply_calibration_ratio(10, ratio), Some(15));
        assert!(ratio > CalibrationRatio::IDENTITY);
    }

    #[test]
    fn profile_is_fixed_typed_and_epoch_bound() {
        let ratio = CalibrationRatio::new(9, 8).expect("ratio");
        let profile = PlannerCalibrationProfile::IDENTITY.with_ratio(
            super::PlannerCalibrationEpoch(1),
            PlannerCalibrationClass::Columnar,
            ratio,
        );
        assert_eq!(profile.epoch.0, 1);
        assert_eq!(profile.ratio(PlannerCalibrationClass::Columnar), ratio);
        assert_eq!(
            profile.ratio(PlannerCalibrationClass::SeqScan),
            CalibrationRatio::IDENTITY
        );
    }

    #[test]
    fn planning_cost_overflow_falls_back_to_base() {
        assert_eq!(
            super::effective_planning_cost(u128::MAX, CalibrationRatio::DOUBLE),
            u128::MAX
        );
    }
}
