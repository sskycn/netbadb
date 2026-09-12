use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use netbadb_core::{
    AdaptiveEvidencePoolHealth, AdaptiveEvidenceRecordError, AdaptiveEvidenceRecordOutcome,
    AdaptiveEvidenceRotationError, AdaptiveEvidenceRotationReport, AdaptiveEvidenceWindowEpoch,
    AutomaticEvidenceRenewalReason, AutomaticOrchestrationStopReason, AutomaticSchedulerDelayClass,
    AutomaticSchedulerFault, AutomaticSchedulerGate, PhysicalColumnarRecommendationInspection,
    PhysicalDesignAdvisorError, PhysicalDesignAdvisorReport, PhysicalDesignCandidateDecision,
    PhysicalDesignEvidenceRecordError, PhysicalDesignEvidenceRecordOutcome,
    PhysicalDesignEvidenceSummary, PhysicalDesignNoActionReason, PhysicalIndexCandidate,
    PhysicalIndexDesignApplyError, PhysicalIndexDesignProposalError,
    PhysicalIndexRecommendationInspection,
};
use netbadb_types::{ColumnId, IndexName, TableId};
use serde::{Deserialize, Serialize};

use crate::physical_design::{
    ServerApprovedPhysicalIndexApplyOutcome, ServerApprovedPhysicalIndexApplyReport,
};
use crate::{
    ServerAdaptiveControlError, ServerAdaptiveControlHandle, ServerAdaptiveMode,
    ServerAdaptiveStatus, ServerPhysicalDesignControlError, ServerPhysicalDesignControlHandle,
    ServerPhysicalDesignRotationReport, ServerPhysicalDesignStatus,
};

pub const OPERATOR_PROTOCOL_VERSION: u16 = 3;
pub const MAX_OPERATOR_PAYLOAD_BYTES: u32 = 64 * 1024;

const OPERATOR_MAGIC: [u8; 4] = *b"NBOP";
const OPERATOR_HEADER_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OperatorPhysicalDesignRuntimeToken([u8; 16]);

impl OperatorPhysicalDesignRuntimeToken {
    fn generate() -> Result<Self, getrandom::Error> {
        let mut bytes = [0_u8; 16];
        getrandom::getrandom(&mut bytes)?;
        Ok(Self(bytes))
    }

    fn parse(value: &str) -> Option<Self> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return None;
        }
        let mut bytes = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = hex_nibble(pair[0])? << 4 | hex_nibble(pair[1])?;
        }
        Some(Self(bytes))
    }

    fn encode(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(32);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Runtime-ready local operator configuration resolved from the deployment
/// manifest. The socket path is absolute and its parent already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOperatorConfig {
    unix_socket: PathBuf,
    io_timeout: Duration,
    allow_physical_index_apply: bool,
}

impl ServerOperatorConfig {
    pub(crate) fn new(
        unix_socket: PathBuf,
        io_timeout: Duration,
        allow_physical_index_apply: bool,
    ) -> Result<Self, ServerOperatorConfigError> {
        if io_timeout.is_zero() {
            return Err(ServerOperatorConfigError::ZeroIoTimeout);
        }
        Ok(Self {
            unix_socket,
            io_timeout,
            allow_physical_index_apply,
        })
    }

    #[must_use]
    pub fn unix_socket(&self) -> &Path {
        &self.unix_socket
    }

    #[must_use]
    pub const fn io_timeout(&self) -> Duration {
        self.io_timeout
    }

    #[must_use]
    pub const fn allow_physical_index_apply(&self) -> bool {
        self.allow_physical_index_apply
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerOperatorConfigError {
    ZeroIoTimeout,
}

impl fmt::Display for ServerOperatorConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIoTimeout => formatter.write_str("operator I/O timeout must be nonzero"),
        }
    }
}

impl Error for ServerOperatorConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAdaptiveModeV3 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV3 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV3 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV3 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    StaleTargetGenerationEvidence,
    StaleTargetIdentityEvidence,
    RetiredTargetEvidence,
    StaleCalibrationEpochEvidence,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerDelayClassV3 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV3 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV3 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV3 {
    Open {
        delay_class: OperatorSchedulerDelayClassV3,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV3,
    },
    Faulted {
        fault: OperatorSchedulerFaultV3,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV3 {
    NoReadyWork,
    StepLimitReached,
    ActiveTrial,
    TrialBoundaryResolved,
    EvidenceRenewalRecommended,
    SelectedCandidateDidNotProgress,
    MaintenanceEnvelopeExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorFeedbackStatusV3 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV3>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV3>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV3 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV3,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV3>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV3 {
    pub mode: OperatorAdaptiveModeV3,
    pub feedback: OperatorFeedbackStatusV3,
    pub driver: Option<OperatorDriverStatusV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV3 {
    pub adaptive: Option<OperatorAdaptiveStatusV3>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyStatusV3 {
    pub enabled: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV3 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV3 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV3 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV3>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV3>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV3 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV3 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV3,
    pub epoch: u64,
    pub schema_generation: Option<u64>,
    pub first_global_commit_seq: Option<u64>,
    pub last_global_commit_seq: Option<u64>,
    pub ordering_high_water: Option<u64>,
    pub recorded_reports: u64,
    pub index_candidate_count: u64,
    pub columnar_candidate_count: u64,
    pub capacity_rejections: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignStatusV3 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV3,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV3,
    pub physical_index_apply: OperatorPhysicalIndexApplyStatusV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV3 {
    pub report_count: u64,
    pub distinct_query_shapes: u64,
    pub total_actual_scan_work_units: u64,
    pub total_rows_examined: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignNoActionReasonV3 {
    BelowMinimumReports,
    BelowMinimumShapeDiversity,
    BelowMinimumActualWork,
    ExistingDesignCovers,
    UnsupportedCurrentLayout,
    IncompleteEvidence,
    CurrentProjectionUnavailable,
    RecommendationLimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignDecisionV3 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV3,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV3 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV3,
    pub decision: OperatorPhysicalDesignDecisionV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV3 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV3,
    pub decision: OperatorPhysicalDesignDecisionV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV3 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV3>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRecommendationsV3 {
    pub runtime_token: Option<String>,
    pub report: OperatorPhysicalDesignAdvisorReportV3,
}

impl std::ops::Deref for OperatorPhysicalDesignRecommendationsV3 {
    type Target = OperatorPhysicalDesignAdvisorReportV3;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalIndexApplyOutcomeV3 {
    Created { index_id: u64 },
    AlreadyApplied { index_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyResultV3 {
    pub table_id: u64,
    pub column_id: u32,
    pub index_name: String,
    pub outcome: OperatorPhysicalIndexApplyOutcomeV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV3 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV3 {
    pub previous_window_epoch: u64,
    pub new_window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub ordering_high_water: Option<u64>,
    pub discarded_target_window_count: u64,
    pub discarded_calibration_epoch_count: u64,
    pub discarded_query_shape_count: u64,
    pub was_incomplete: bool,
    pub was_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorErrorCodeV3 {
    AdaptiveNotEnabled,
    DriverNotEnabled,
    SchedulerNotFaulted,
    EvidenceWindowChanged,
    EvidenceWindowEpochExhausted,
    PhysicalDesignNotEnabled,
    PhysicalDesignEvidenceEpochChanged,
    PhysicalDesignEvidenceEpochExhausted,
    PhysicalDesignNoEvidence,
    PhysicalDesignStaleSchema,
    PhysicalDesignInconclusiveCapacity,
    PhysicalIndexApplyNotEnabled,
    PhysicalDesignRuntimeChanged,
    InvalidIndexName,
    PhysicalIndexCandidateNotObserved,
    PhysicalIndexNotRecommended,
    PhysicalIndexNameConflict,
    PhysicalIndexApplyFailed,
    ResponseTooLarge,
    ServerStopped,
    MalformedRequest,
    UnsupportedProtocolVersion,
    RequestTooLarge,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorRemoteErrorV3 {
    pub code: OperatorErrorCodeV3,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorRequestV3 {
    request_id: u64,
    operation: OperatorOperationV3,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorOperationV3 {
    Status {},
    RotateEvidence {
        expected_window_epoch: u64,
    },
    ResetFaultedScheduler {},
    PhysicalDesignRecommendations {},
    RotatePhysicalDesignEvidence {
        expected_evidence_epoch: u64,
    },
    ApplyPhysicalIndex {
        expected_runtime_token: String,
        expected_evidence_epoch: u64,
        table_id: u64,
        column_id: u32,
        index_name: String,
    },
}

struct OperatorPhysicalIndexApplyInput {
    expected_runtime_token: String,
    expected_evidence_epoch: u64,
    table_id: u64,
    column_id: u32,
    index_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResponseV3 {
    Ok {
        request_id: u64,
        result: OperatorResultV3,
    },
    Error {
        request_id: u64,
        error: OperatorRemoteErrorV3,
    },
}

impl OperatorResponseV3 {
    const fn request_id(&self) -> u64 {
        match self {
            Self::Ok { request_id, .. } | Self::Error { request_id, .. } => *request_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResultV3 {
    Status {
        status: Box<OperatorStatusV3>,
    },
    EvidenceRotated {
        rotation: OperatorEvidenceRotationV3,
    },
    SchedulerReset {},
    PhysicalDesignRecommendations {
        runtime_token: Option<String>,
        report: OperatorPhysicalDesignAdvisorReportV3,
    },
    PhysicalDesignEvidenceRotated {
        rotation: OperatorPhysicalDesignRotationV3,
    },
    PhysicalIndexApplied {
        apply: OperatorPhysicalIndexApplyResultV3,
    },
}

#[derive(Debug)]
pub enum OperatorProtocolError {
    Io(io::Error),
    TruncatedHeader,
    TruncatedPayload,
    WrongMagic,
    UnsupportedVersion(u16),
    NonzeroReserved(u16),
    RequestTooLarge(u32),
    PayloadTooLarge(usize),
    InvalidJson(serde_json::Error),
}

impl fmt::Display for OperatorProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::TruncatedHeader => formatter.write_str("truncated NBOP frame header"),
            Self::TruncatedPayload => formatter.write_str("truncated NBOP frame payload"),
            Self::WrongMagic => formatter.write_str("invalid NBOP frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported NBOP protocol version {version}")
            }
            Self::NonzeroReserved(value) => {
                write!(formatter, "NBOP reserved header field is nonzero ({value})")
            }
            Self::RequestTooLarge(length) => write!(
                formatter,
                "NBOP payload length {length} exceeds {MAX_OPERATOR_PAYLOAD_BYTES} bytes"
            ),
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "NBOP response payload length {length} is too large"
                )
            }
            Self::InvalidJson(error) => write!(formatter, "invalid NBOP JSON payload: {error}"),
        }
    }
}

impl Error for OperatorProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidJson(error) => Some(error),
            _ => None,
        }
    }
}

fn read_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
) -> Result<T, OperatorProtocolError> {
    let mut header = [0_u8; OPERATOR_HEADER_BYTES];
    read_exact_frame(reader, &mut header, OperatorProtocolError::TruncatedHeader)?;
    if header[..4] != OPERATOR_MAGIC {
        return Err(OperatorProtocolError::WrongMagic);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != OPERATOR_PROTOCOL_VERSION {
        return Err(OperatorProtocolError::UnsupportedVersion(version));
    }
    let reserved = u16::from_be_bytes([header[6], header[7]]);
    if reserved != 0 {
        return Err(OperatorProtocolError::NonzeroReserved(reserved));
    }
    let length = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    if length > MAX_OPERATOR_PAYLOAD_BYTES {
        return Err(OperatorProtocolError::RequestTooLarge(length));
    }
    let mut payload = vec![0_u8; length as usize];
    read_exact_frame(
        reader,
        &mut payload,
        OperatorProtocolError::TruncatedPayload,
    )?;
    serde_json::from_slice(&payload).map_err(OperatorProtocolError::InvalidJson)
}

fn read_exact_frame(
    reader: &mut impl Read,
    bytes: &mut [u8],
    truncated: OperatorProtocolError,
) -> Result<(), OperatorProtocolError> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            truncated
        } else {
            OperatorProtocolError::Io(error)
        }
    })
}

fn write_frame<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
) -> Result<(), OperatorProtocolError> {
    let payload = serde_json::to_vec(value).map_err(OperatorProtocolError::InvalidJson)?;
    if payload.len() > MAX_OPERATOR_PAYLOAD_BYTES as usize {
        return Err(OperatorProtocolError::PayloadTooLarge(payload.len()));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| OperatorProtocolError::PayloadTooLarge(payload.len()))?;
    let mut header = [0_u8; OPERATOR_HEADER_BYTES];
    header[..4].copy_from_slice(&OPERATOR_MAGIC);
    header[4..6].copy_from_slice(&OPERATOR_PROTOCOL_VERSION.to_be_bytes());
    header[8..12].copy_from_slice(&length.to_be_bytes());
    writer
        .write_all(&header)
        .and_then(|()| writer.write_all(&payload))
        .and_then(|()| writer.flush())
        .map_err(OperatorProtocolError::Io)
}

#[derive(Debug)]
pub enum OperatorClientError {
    OperatorNotConfigured,
    UnsupportedPlatform,
    Connect { path: PathBuf, source: io::Error },
    Configure(io::Error),
    Protocol(OperatorProtocolError),
    RequestIdMismatch { expected: u64, received: u64 },
    UnexpectedResult,
    Remote(OperatorRemoteErrorV3),
}

impl fmt::Display for OperatorClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperatorNotConfigured => {
                formatter.write_str("operator plane is not configured in the manifest")
            }
            Self::UnsupportedPlatform => {
                formatter.write_str("the local operator plane requires a Unix platform")
            }
            Self::Connect { path, source } => write!(
                formatter,
                "failed to connect to operator socket `{}`: {source}",
                path.display()
            ),
            Self::Configure(error) => {
                write!(formatter, "failed to configure operator socket: {error}")
            }
            Self::Protocol(error) => error.fmt(formatter),
            Self::RequestIdMismatch { expected, received } => write!(
                formatter,
                "operator response request ID {received} does not match {expected}"
            ),
            Self::UnexpectedResult => formatter.write_str("operator returned an unexpected result"),
            Self::Remote(error) => write!(formatter, "operator request failed: {}", error.message),
        }
    }
}

impl Error for OperatorClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect { source, .. } | Self::Configure(source) => Some(source),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

pub struct ServerOperatorClient<'a> {
    config: &'a ServerOperatorConfig,
}

impl<'a> ServerOperatorClient<'a> {
    #[must_use]
    pub const fn new(config: &'a ServerOperatorConfig) -> Self {
        Self { config }
    }

    pub fn status(&self) -> Result<OperatorStatusV3, OperatorClientError> {
        match self.exchange(OperatorOperationV3::Status {})? {
            OperatorResultV3::Status { status } => Ok(*status),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_evidence(
        &self,
        expected_window_epoch: u64,
    ) -> Result<OperatorEvidenceRotationV3, OperatorClientError> {
        match self.exchange(OperatorOperationV3::RotateEvidence {
            expected_window_epoch,
        })? {
            OperatorResultV3::EvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn reset_faulted_scheduler(&self) -> Result<(), OperatorClientError> {
        match self.exchange(OperatorOperationV3::ResetFaultedScheduler {})? {
            OperatorResultV3::SchedulerReset {} => Ok(()),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn physical_design_recommendations(
        &self,
    ) -> Result<OperatorPhysicalDesignRecommendationsV3, OperatorClientError> {
        match self.exchange(OperatorOperationV3::PhysicalDesignRecommendations {})? {
            OperatorResultV3::PhysicalDesignRecommendations {
                runtime_token,
                report,
            } => Ok(OperatorPhysicalDesignRecommendationsV3 {
                runtime_token,
                report,
            }),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_physical_design_evidence(
        &self,
        expected_evidence_epoch: u64,
    ) -> Result<OperatorPhysicalDesignRotationV3, OperatorClientError> {
        match self.exchange(OperatorOperationV3::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        })? {
            OperatorResultV3::PhysicalDesignEvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn apply_physical_index(
        &self,
        expected_runtime_token: impl Into<String>,
        expected_evidence_epoch: u64,
        table_id: u64,
        column_id: u32,
        index_name: impl Into<String>,
    ) -> Result<OperatorPhysicalIndexApplyResultV3, OperatorClientError> {
        match self.exchange(OperatorOperationV3::ApplyPhysicalIndex {
            expected_runtime_token: expected_runtime_token.into(),
            expected_evidence_epoch,
            table_id,
            column_id,
            index_name: index_name.into(),
        })? {
            OperatorResultV3::PhysicalIndexApplied { apply } => Ok(apply),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    #[cfg(unix)]
    fn exchange(
        &self,
        operation: OperatorOperationV3,
    ) -> Result<OperatorResultV3, OperatorClientError> {
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(self.config.unix_socket()).map_err(|source| {
            OperatorClientError::Connect {
                path: self.config.unix_socket().to_path_buf(),
                source,
            }
        })?;
        stream
            .set_read_timeout(Some(self.config.io_timeout()))
            .and_then(|()| stream.set_write_timeout(Some(self.config.io_timeout())))
            .map_err(OperatorClientError::Configure)?;
        let request_id = 1;
        write_frame(
            &mut stream,
            &OperatorRequestV3 {
                request_id,
                operation,
            },
        )
        .map_err(OperatorClientError::Protocol)?;
        let response: OperatorResponseV3 =
            read_frame(&mut stream).map_err(OperatorClientError::Protocol)?;
        match response {
            OperatorResponseV3::Ok {
                request_id: received,
                result,
            } => {
                verify_request_id(request_id, received)?;
                Ok(result)
            }
            OperatorResponseV3::Error {
                request_id: received,
                error,
            } => {
                if received != 0 {
                    verify_request_id(request_id, received)?;
                }
                Err(OperatorClientError::Remote(error))
            }
        }
    }

    #[cfg(not(unix))]
    fn exchange(
        &self,
        _operation: OperatorOperationV3,
    ) -> Result<OperatorResultV3, OperatorClientError> {
        Err(OperatorClientError::UnsupportedPlatform)
    }
}

fn verify_request_id(expected: u64, received: u64) -> Result<(), OperatorClientError> {
    if expected == received {
        Ok(())
    } else {
        Err(OperatorClientError::RequestIdMismatch { expected, received })
    }
}

#[derive(Debug)]
pub enum ServerOperatorError {
    UnsupportedPlatform,
    Randomness(getrandom::Error),
    PathExists(PathBuf),
    Bind { path: PathBuf, source: io::Error },
    Configure { path: PathBuf, source: io::Error },
    ThreadSpawn(io::Error),
    Accept(io::Error),
    SocketPathReplaced(PathBuf),
    Cleanup { path: PathBuf, source: io::Error },
    ThreadPanicked,
}

impl fmt::Display for ServerOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => {
                formatter.write_str("configured operator plane requires a Unix platform")
            }
            Self::Randomness(_) => {
                formatter.write_str("failed to generate the operator physical-design runtime token")
            }
            Self::PathExists(path) => write!(
                formatter,
                "operator socket path `{}` already exists; verify the old daemon is stopped and remove it explicitly",
                path.display()
            ),
            Self::Bind { path, source } => write!(
                formatter,
                "failed to bind operator socket `{}`: {source}",
                path.display()
            ),
            Self::Configure { path, source } => write!(
                formatter,
                "failed to configure operator socket `{}`: {source}",
                path.display()
            ),
            Self::ThreadSpawn(error) => {
                write!(formatter, "failed to spawn operator listener: {error}")
            }
            Self::Accept(error) => write!(formatter, "operator socket accept failed: {error}"),
            Self::SocketPathReplaced(path) => write!(
                formatter,
                "operator socket path `{}` was replaced and was not removed",
                path.display()
            ),
            Self::Cleanup { path, source } => write!(
                formatter,
                "failed to clean up operator socket `{}`: {source}",
                path.display()
            ),
            Self::ThreadPanicked => formatter.write_str("operator listener thread panicked"),
        }
    }
}

impl Error for ServerOperatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. }
            | Self::Configure { source, .. }
            | Self::Cleanup { source, .. }
            | Self::ThreadSpawn(source)
            | Self::Accept(source) => Some(source),
            _ => None,
        }
    }
}

pub(crate) struct ServerOperatorPlane {
    shutdown: Sender<()>,
    join: Option<std::thread::JoinHandle<Result<(), ServerOperatorError>>>,
}

impl ServerOperatorPlane {
    #[cfg(unix)]
    pub(crate) fn start(
        config: ServerOperatorConfig,
        adaptive_control: ServerAdaptiveControlHandle,
        physical_design_control: ServerPhysicalDesignControlHandle,
        failure_notification: Sender<()>,
    ) -> Result<Self, ServerOperatorError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
        use std::os::unix::net::UnixListener;
        use std::sync::mpsc;

        let runtime_token = config
            .allow_physical_index_apply()
            .then(OperatorPhysicalDesignRuntimeToken::generate)
            .transpose()
            .map_err(ServerOperatorError::Randomness)?;
        let path = config.unix_socket().to_path_buf();
        match std::fs::symlink_metadata(&path) {
            Ok(_) => return Err(ServerOperatorError::PathExists(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ServerOperatorError::Bind { path, source });
            }
        }
        let listener = UnixListener::bind(&path).map_err(|source| ServerOperatorError::Bind {
            path: path.clone(),
            source,
        })?;
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| ServerOperatorError::Configure {
                path: path.clone(),
                source,
            })?;
        if !metadata.file_type().is_socket() {
            return Err(ServerOperatorError::SocketPathReplaced(path));
        }
        let owned = OwnedSocketPath {
            path: path.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            armed: true,
        };
        if let Err(source) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        {
            return Err(ServerOperatorError::Configure { path, source });
        }
        match std::fs::symlink_metadata(&path) {
            Ok(current)
                if current.file_type().is_socket()
                    && current.dev() == owned.device
                    && current.ino() == owned.inode => {}
            Ok(_) => return Err(ServerOperatorError::SocketPathReplaced(path)),
            Err(source) => return Err(ServerOperatorError::Configure { path, source }),
        }
        if let Err(source) = listener.set_nonblocking(true) {
            return Err(ServerOperatorError::Configure { path, source });
        }
        let (shutdown, shutdown_rx) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("netbadb-operator-listener".into())
            .spawn(move || {
                let mut failure = OperatorFailureNotification::new(failure_notification);
                let run = run_operator_listener(
                    &listener,
                    &shutdown_rx,
                    config.io_timeout(),
                    &adaptive_control,
                    &physical_design_control,
                    config.allow_physical_index_apply(),
                    runtime_token,
                );
                let result = run.and(owned.cleanup());
                if result.is_ok() {
                    failure.disarm();
                }
                result
            })
            .map_err(ServerOperatorError::ThreadSpawn)?;
        Ok(Self {
            shutdown,
            join: Some(join),
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn start(
        _config: ServerOperatorConfig,
        _adaptive_control: ServerAdaptiveControlHandle,
        _physical_design_control: ServerPhysicalDesignControlHandle,
        _failure_notification: Sender<()>,
    ) -> Result<Self, ServerOperatorError> {
        Err(ServerOperatorError::UnsupportedPlatform)
    }

    pub(crate) fn shutdown(mut self) -> Result<(), ServerOperatorError> {
        let _ = self.shutdown.send(());
        let join = self
            .join
            .take()
            .ok_or(ServerOperatorError::ThreadPanicked)?;
        join.join()
            .map_err(|_| ServerOperatorError::ThreadPanicked)?
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }
}

struct OperatorFailureNotification {
    sender: Sender<()>,
    armed: bool,
}

impl OperatorFailureNotification {
    const fn new(sender: Sender<()>) -> Self {
        Self {
            sender,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OperatorFailureNotification {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.sender.send(());
        }
    }
}

#[cfg(unix)]
struct OwnedSocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
    armed: bool,
}

#[cfg(unix)]
impl OwnedSocketPath {
    fn cleanup(mut self) -> Result<(), ServerOperatorError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode =>
            {
                std::fs::remove_file(&self.path).map_err(|source| {
                    ServerOperatorError::Cleanup {
                        path: self.path.clone(),
                        source,
                    }
                })?;
                self.armed = false;
                Ok(())
            }
            Ok(_) => {
                self.armed = false;
                Err(ServerOperatorError::SocketPathReplaced(self.path.clone()))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.armed = false;
                Ok(())
            }
            Err(source) => {
                self.armed = false;
                Err(ServerOperatorError::Cleanup {
                    path: self.path.clone(),
                    source,
                })
            }
        }
    }
}

#[cfg(unix)]
impl Drop for OwnedSocketPath {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        if !self.armed {
            return;
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
            {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

#[cfg(unix)]
fn run_operator_listener(
    listener: &std::os::unix::net::UnixListener,
    shutdown: &std::sync::mpsc::Receiver<()>,
    io_timeout: Duration,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> Result<(), ServerOperatorError> {
    use std::sync::mpsc::TryRecvError;

    loop {
        match shutdown.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => return Ok(()),
            Err(TryRecvError::Empty) => {}
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                if stream
                    .set_nonblocking(false)
                    .and_then(|()| stream.set_read_timeout(Some(io_timeout)))
                    .and_then(|()| stream.set_write_timeout(Some(io_timeout)))
                    .is_ok()
                {
                    let _ = serve_operator_connection(
                        &mut stream,
                        adaptive_control,
                        physical_design_control,
                        allow_physical_index_apply,
                        runtime_token,
                    );
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(ServerOperatorError::Accept(error)),
        }
    }
}

#[cfg(unix)]
fn serve_operator_connection(
    stream: &mut std::os::unix::net::UnixStream,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> Result<(), OperatorProtocolError> {
    let request = match read_frame::<OperatorRequestV3>(stream) {
        Ok(request) => request,
        Err(error) => {
            let response = OperatorResponseV3::Error {
                request_id: 0,
                error: protocol_remote_error(&error),
            };
            let _ = write_frame(stream, &response);
            return Err(error);
        }
    };
    let response = execute_operator_request(
        request,
        adaptive_control,
        physical_design_control,
        allow_physical_index_apply,
        runtime_token,
    );
    write_operator_response(stream, &response)
}

fn write_operator_response(
    writer: &mut impl Write,
    response: &OperatorResponseV3,
) -> Result<(), OperatorProtocolError> {
    let request_id = response.request_id();
    match write_frame(writer, response) {
        Err(OperatorProtocolError::PayloadTooLarge(_)) => write_frame(
            writer,
            &OperatorResponseV3::Error {
                request_id,
                error: response_too_large_error(),
            },
        ),
        result => result,
    }
}

fn execute_operator_request(
    request: OperatorRequestV3,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> OperatorResponseV3 {
    let result = match request.operation {
        OperatorOperationV3::Status {} => operator_status(
            adaptive_control,
            physical_design_control,
            allow_physical_index_apply,
            runtime_token,
        )
        .map(|status| OperatorResultV3::Status {
            status: Box::new(status),
        }),
        OperatorOperationV3::RotateEvidence {
            expected_window_epoch,
        } => adaptive_control
            .rotate_evidence_if_window(AdaptiveEvidenceWindowEpoch(expected_window_epoch))
            .map(operator_rotation)
            .map(|rotation| OperatorResultV3::EvidenceRotated { rotation })
            .map_err(control_remote_error),
        OperatorOperationV3::ResetFaultedScheduler {} => adaptive_control
            .reset_faulted_scheduler()
            .map(|()| OperatorResultV3::SchedulerReset {})
            .map_err(control_remote_error),
        OperatorOperationV3::PhysicalDesignRecommendations {} => physical_design_control
            .recommendations()
            .map(operator_physical_design_report)
            .map(|report| OperatorResultV3::PhysicalDesignRecommendations {
                runtime_token: runtime_token.map(OperatorPhysicalDesignRuntimeToken::encode),
                report,
            })
            .map_err(physical_design_remote_error),
        OperatorOperationV3::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        } => physical_design_control
            .rotate_evidence_if_epoch(netbadb_core::PhysicalDesignEvidenceEpoch(
                expected_evidence_epoch,
            ))
            .map(operator_physical_design_rotation)
            .map(|rotation| OperatorResultV3::PhysicalDesignEvidenceRotated { rotation })
            .map_err(physical_design_remote_error),
        OperatorOperationV3::ApplyPhysicalIndex {
            expected_runtime_token,
            expected_evidence_epoch,
            table_id,
            column_id,
            index_name,
        } => execute_physical_index_apply(
            physical_design_control,
            allow_physical_index_apply,
            runtime_token,
            OperatorPhysicalIndexApplyInput {
                expected_runtime_token,
                expected_evidence_epoch,
                table_id,
                column_id,
                index_name,
            },
        ),
    };
    match result {
        Ok(result) => OperatorResponseV3::Ok {
            request_id: request.request_id,
            result,
        },
        Err(error) => OperatorResponseV3::Error {
            request_id: request.request_id,
            error,
        },
    }
}

fn execute_physical_index_apply(
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
    input: OperatorPhysicalIndexApplyInput,
) -> Result<OperatorResultV3, OperatorRemoteErrorV3> {
    if !allow_physical_index_apply {
        return Err(OperatorRemoteErrorV3 {
            code: OperatorErrorCodeV3::PhysicalIndexApplyNotEnabled,
            message: "operator physical-index apply is not enabled by the manifest".into(),
        });
    }
    let expected = OperatorPhysicalDesignRuntimeToken::parse(&input.expected_runtime_token)
        .ok_or_else(|| OperatorRemoteErrorV3 {
            code: OperatorErrorCodeV3::MalformedRequest,
            message: "expected_runtime_token must be exactly 32 lowercase hexadecimal characters"
                .into(),
        })?;
    let runtime_token_matches = runtime_token == Some(expected);
    let index_name = IndexName::new(input.index_name).map_err(|_| OperatorRemoteErrorV3 {
        code: OperatorErrorCodeV3::InvalidIndexName,
        message: "index_name must be nonempty and at most 255 bytes".into(),
    })?;
    physical_design_control
        .apply_approved_index(
            runtime_token_matches,
            netbadb_core::PhysicalDesignEvidenceEpoch(input.expected_evidence_epoch),
            PhysicalIndexCandidate {
                table_id: TableId(input.table_id),
                column_id: ColumnId(input.column_id),
            },
            index_name,
        )
        .map(operator_physical_index_apply_result)
        .map(|apply| OperatorResultV3::PhysicalIndexApplied { apply })
        .map_err(physical_design_remote_error)
}

fn operator_physical_index_apply_result(
    report: ServerApprovedPhysicalIndexApplyReport,
) -> OperatorPhysicalIndexApplyResultV3 {
    let outcome = match report.outcome {
        ServerApprovedPhysicalIndexApplyOutcome::Created { index_id } => {
            OperatorPhysicalIndexApplyOutcomeV3::Created {
                index_id: index_id.0,
            }
        }
        ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id } => {
            OperatorPhysicalIndexApplyOutcomeV3::AlreadyApplied {
                index_id: index_id.0,
            }
        }
        ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered => {
            OperatorPhysicalIndexApplyOutcomeV3::AlreadyCovered
        }
    };
    OperatorPhysicalIndexApplyResultV3 {
        table_id: report.candidate.table_id.0,
        column_id: report.candidate.column_id.0,
        index_name: report.index_name.as_str().to_owned(),
        outcome,
    }
}

fn operator_status(
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> Result<OperatorStatusV3, OperatorRemoteErrorV3> {
    let adaptive = match adaptive_control.status() {
        Ok(status) if status.mode == ServerAdaptiveMode::Disabled => None,
        Ok(status) => Some(operator_adaptive_status(status).map_err(control_remote_error)?),
        Err(ServerAdaptiveControlError::AdaptiveNotEnabled) => None,
        Err(error) => return Err(control_remote_error(error)),
    };
    let physical_design = match physical_design_control.status() {
        Ok(status) => Some(operator_physical_design_status(
            status,
            allow_physical_index_apply,
            runtime_token,
        )),
        Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled) => None,
        Err(error) => return Err(physical_design_remote_error(error)),
    };
    if adaptive.is_none() && physical_design.is_none() {
        return Err(OperatorRemoteErrorV3 {
            code: OperatorErrorCodeV3::Internal,
            message: "operator plane has no managed runtime".into(),
        });
    }
    Ok(OperatorStatusV3 {
        adaptive,
        physical_design,
    })
}

fn operator_adaptive_status(
    status: ServerAdaptiveStatus,
) -> Result<OperatorAdaptiveStatusV3, ServerAdaptiveControlError> {
    let mode = match status.mode {
        ServerAdaptiveMode::FeedbackOnly => OperatorAdaptiveModeV3::FeedbackOnly,
        ServerAdaptiveMode::Driven => OperatorAdaptiveModeV3::Driven,
        ServerAdaptiveMode::Disabled => return Err(ServerAdaptiveControlError::AdaptiveNotEnabled),
    };
    let feedback = status
        .feedback
        .ok_or(ServerAdaptiveControlError::AdaptiveNotEnabled)?;
    let progress = feedback.evidence_progress;
    let feedback = OperatorFeedbackStatusV3 {
        eligible_query_count: feedback.eligible_query_count,
        record_success_count: feedback.record_success_count,
        record_error_count: feedback.record_error_count,
        capacity_rejection_count: feedback.capacity_rejection_count,
        schema_rotation_count: feedback.schema_rotation_count,
        incomplete_report_count: feedback.incomplete_report_count,
        counter_overflowed: feedback.counter_overflowed,
        last_record_outcome: feedback.last_record_outcome.map(record_outcome),
        last_record_error: feedback.last_record_error.map(record_error),
        window_epoch: progress.window_epoch.0,
        schema_generation: progress.schema_generation.map(|generation| generation.0),
        recorded_reports: progress.recorded_reports,
        pool_health: match feedback.pool_health {
            AdaptiveEvidencePoolHealth::Healthy => OperatorEvidencePoolHealthV3::Healthy,
            AdaptiveEvidencePoolHealth::RotationRecommended => {
                OperatorEvidencePoolHealthV3::RotationRecommended
            }
        },
    };
    let driver = status.driver.map(|driver| OperatorDriverStatusV3 {
        scheduler_last_observed_tick: driver.scheduler_state.last_observed_tick.map(|tick| tick.0),
        scheduler_last_run_tick: driver.scheduler_state.last_run_tick.map(|tick| tick.0),
        scheduler_gate: scheduler_gate(driver.scheduler_state.gate),
        last_submitted_logical_tick: driver.last_submitted_logical_tick.map(|tick| tick.0),
        tick_pending: driver.tick_pending,
        driver_tick_count: driver.driver_tick_count,
        scheduler_tick_count: driver.scheduler_tick_count,
        scheduler_ran_count: driver.scheduler_ran_count,
        scheduler_held_count: driver.scheduler_held_count,
        scheduler_error_count: driver.scheduler_error_count,
        last_orchestration_stop_reason: driver
            .last_orchestration_stop_reason
            .map(orchestration_stop_reason),
        host_clock_exhausted: driver.host_clock_exhausted,
        counter_overflowed: driver.counter_overflowed,
    });
    Ok(OperatorAdaptiveStatusV3 {
        mode,
        feedback,
        driver,
    })
}

fn record_outcome(outcome: AdaptiveEvidenceRecordOutcome) -> OperatorEvidenceRecordOutcomeV3 {
    match outcome {
        AdaptiveEvidenceRecordOutcome::Recorded => OperatorEvidenceRecordOutcomeV3::Recorded,
        AdaptiveEvidenceRecordOutcome::SchemaRotated => {
            OperatorEvidenceRecordOutcomeV3::SchemaRotated
        }
        AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV3::RecordedWithCapacityRejection
        }
        AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV3::SchemaRotatedWithCapacityRejection
        }
    }
}

fn record_error(error: AdaptiveEvidenceRecordError) -> OperatorEvidenceRecordErrorV3 {
    match error {
        AdaptiveEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorEvidenceRecordErrorV3::GlobalVisibilityRequired
        }
        AdaptiveEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorEvidenceRecordErrorV3::StaleSchemaEvidence
        }
        AdaptiveEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorEvidenceRecordErrorV3::OutOfOrderVisibility
        }
        AdaptiveEvidenceRecordError::StaleTargetGenerationEvidence { .. } => {
            OperatorEvidenceRecordErrorV3::StaleTargetGenerationEvidence
        }
        AdaptiveEvidenceRecordError::StaleTargetIdentityEvidence { .. } => {
            OperatorEvidenceRecordErrorV3::StaleTargetIdentityEvidence
        }
        AdaptiveEvidenceRecordError::RetiredTargetEvidence { .. } => {
            OperatorEvidenceRecordErrorV3::RetiredTargetEvidence
        }
        AdaptiveEvidenceRecordError::StaleCalibrationEpochEvidence { .. } => {
            OperatorEvidenceRecordErrorV3::StaleCalibrationEpochEvidence
        }
        AdaptiveEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorEvidenceRecordErrorV3::EvidenceWindowEpochExhausted
        }
    }
}

fn scheduler_gate(gate: AutomaticSchedulerGate) -> OperatorSchedulerGateV3 {
    match gate {
        AutomaticSchedulerGate::Open { delay } => OperatorSchedulerGateV3::Open {
            delay_class: match delay {
                AutomaticSchedulerDelayClass::Normal => OperatorSchedulerDelayClassV3::Normal,
                AutomaticSchedulerDelayClass::Idle => OperatorSchedulerDelayClassV3::Idle,
                AutomaticSchedulerDelayClass::NoProgress => {
                    OperatorSchedulerDelayClassV3::NoProgress
                }
            },
        },
        AutomaticSchedulerGate::AwaitingTrialProgress { evidence } => {
            OperatorSchedulerGateV3::AwaitingTrialProgress {
                window_epoch: evidence.window_epoch.0,
                schema_generation: evidence.schema_generation.map(|generation| generation.0),
                recorded_reports: evidence.recorded_reports,
            }
        }
        AutomaticSchedulerGate::AwaitingEvidenceRenewal {
            blocked_window_epoch,
            recommendation,
        } => OperatorSchedulerGateV3::AwaitingEvidenceRenewal {
            blocked_window_epoch: blocked_window_epoch.0,
            renewal_reason: match recommendation.reason {
                AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged => {
                    OperatorEvidenceRenewalReasonV3::ColumnarPhysicalStateChanged
                }
                AutomaticEvidenceRenewalReason::ColumnarEligibilityChanged => {
                    OperatorEvidenceRenewalReasonV3::ColumnarEligibilityChanged
                }
                AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged => {
                    OperatorEvidenceRenewalReasonV3::AuthoritativeLsmLayoutChanged
                }
            },
        },
        AutomaticSchedulerGate::Faulted(fault) => OperatorSchedulerGateV3::Faulted {
            fault: match fault {
                AutomaticSchedulerFault::MaintenanceEnvelopeExceeded => {
                    OperatorSchedulerFaultV3::MaintenanceEnvelopeExceeded
                }
                AutomaticSchedulerFault::StepFailed => OperatorSchedulerFaultV3::StepFailed,
                AutomaticSchedulerFault::ConsumptionOverflow => {
                    OperatorSchedulerFaultV3::ConsumptionOverflow
                }
            },
        },
    }
}

fn orchestration_stop_reason(
    reason: AutomaticOrchestrationStopReason,
) -> OperatorOrchestrationStopReasonV3 {
    match reason {
        AutomaticOrchestrationStopReason::NoReadyWork => {
            OperatorOrchestrationStopReasonV3::NoReadyWork
        }
        AutomaticOrchestrationStopReason::StepLimitReached => {
            OperatorOrchestrationStopReasonV3::StepLimitReached
        }
        AutomaticOrchestrationStopReason::ActiveTrial(_) => {
            OperatorOrchestrationStopReasonV3::ActiveTrial
        }
        AutomaticOrchestrationStopReason::TrialBoundaryResolved => {
            OperatorOrchestrationStopReasonV3::TrialBoundaryResolved
        }
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(_) => {
            OperatorOrchestrationStopReasonV3::EvidenceRenewalRecommended
        }
        AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress => {
            OperatorOrchestrationStopReasonV3::SelectedCandidateDidNotProgress
        }
        AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded { .. } => {
            OperatorOrchestrationStopReasonV3::MaintenanceEnvelopeExceeded
        }
    }
}

fn operator_rotation(report: AdaptiveEvidenceRotationReport) -> OperatorEvidenceRotationV3 {
    OperatorEvidenceRotationV3 {
        previous_window_epoch: report.previous_window_epoch.0,
        new_window_epoch: report.new_window_epoch.0,
        schema_generation: report.schema_generation.map(|generation| generation.0),
        ordering_high_water: report.ordering_high_water.map(|sequence| sequence.0),
        discarded_target_window_count: report.discarded_target_window_count,
        discarded_calibration_epoch_count: report.discarded_calibration_epoch_count,
        discarded_query_shape_count: report.discarded_query_shape_count,
        was_incomplete: report.was_incomplete,
        was_truncated: report.was_truncated,
    }
}

fn operator_physical_design_status(
    status: ServerPhysicalDesignStatus,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> OperatorPhysicalDesignStatusV3 {
    let diagnostics = status.diagnostics;
    let evidence = status.evidence;
    OperatorPhysicalDesignStatusV3 {
        diagnostics: OperatorPhysicalDesignDiagnosticsV3 {
            eligible_query_count: diagnostics.eligible_query_count,
            record_success_count: diagnostics.record_success_count,
            record_error_count: diagnostics.record_error_count,
            schema_rotation_count: diagnostics.schema_rotation_count,
            capacity_rejection_count: diagnostics.capacity_rejection_count,
            incomplete_report_count: diagnostics.incomplete_report_count,
            counter_overflowed: diagnostics.counter_overflowed,
            last_record_outcome: diagnostics
                .last_record_outcome
                .map(physical_design_record_outcome),
            last_record_error: diagnostics
                .last_record_error
                .map(physical_design_record_error),
        },
        evidence: OperatorPhysicalDesignEvidenceStatusV3 {
            limits: OperatorPhysicalDesignEvidenceLimitsV3 {
                max_index_candidates: evidence.limits.max_index_candidates,
                max_columnar_candidates: evidence.limits.max_columnar_candidates,
                max_query_shapes_per_candidate: evidence.limits.max_query_shapes_per_candidate,
                max_columnar_columns_per_candidate: evidence
                    .limits
                    .max_columnar_columns_per_candidate,
            },
            epoch: evidence.epoch.0,
            schema_generation: evidence.schema_generation.map(|generation| generation.0),
            first_global_commit_seq: evidence.first_global_commit_seq.map(|sequence| sequence.0),
            last_global_commit_seq: evidence.last_global_commit_seq.map(|sequence| sequence.0),
            ordering_high_water: evidence.ordering_high_water.map(|sequence| sequence.0),
            recorded_reports: evidence.recorded_reports,
            index_candidate_count: evidence.index_candidate_count,
            columnar_candidate_count: evidence.columnar_candidate_count,
            capacity_rejections: evidence.capacity_rejections,
            discarded_incomplete_reports: evidence.discarded_incomplete_reports,
            overflowed: evidence.overflowed,
            incomplete: evidence.incomplete,
            truncated: evidence.truncated,
        },
        physical_index_apply: OperatorPhysicalIndexApplyStatusV3 {
            enabled: allow_physical_index_apply,
            runtime_token: runtime_token.map(OperatorPhysicalDesignRuntimeToken::encode),
        },
    }
}

const fn physical_design_record_outcome(
    outcome: PhysicalDesignEvidenceRecordOutcome,
) -> OperatorPhysicalDesignRecordOutcomeV3 {
    match outcome {
        PhysicalDesignEvidenceRecordOutcome::Recorded => {
            OperatorPhysicalDesignRecordOutcomeV3::Recorded
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotated => {
            OperatorPhysicalDesignRecordOutcomeV3::SchemaRotated
        }
        PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV3::RecordedWithCapacityRejection
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV3::SchemaRotatedWithCapacityRejection
        }
    }
}

const fn physical_design_record_error(
    error: PhysicalDesignEvidenceRecordError,
) -> OperatorPhysicalDesignRecordErrorV3 {
    match error {
        PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorPhysicalDesignRecordErrorV3::GlobalVisibilityRequired
        }
        PhysicalDesignEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorPhysicalDesignRecordErrorV3::StaleSchemaEvidence
        }
        PhysicalDesignEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorPhysicalDesignRecordErrorV3::OutOfOrderVisibility
        }
        PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorPhysicalDesignRecordErrorV3::EvidenceWindowEpochExhausted
        }
    }
}

fn operator_physical_design_report(
    report: PhysicalDesignAdvisorReport,
) -> OperatorPhysicalDesignAdvisorReportV3 {
    OperatorPhysicalDesignAdvisorReportV3 {
        evidence_epoch: report.evidence_epoch.0,
        schema_generation: report.schema_generation.0,
        first_global_commit_seq: report.first_global_commit_seq.0,
        last_global_commit_seq: report.last_global_commit_seq.0,
        recorded_reports: report.recorded_reports,
        discarded_incomplete_reports: report.discarded_incomplete_reports,
        overflowed: report.overflowed,
        incomplete: report.incomplete,
        index_candidates: report
            .index_candidates
            .into_iter()
            .map(operator_index_candidate)
            .collect(),
        columnar_candidates: report
            .columnar_candidates
            .into_iter()
            .map(operator_columnar_candidate)
            .collect(),
    }
}

fn operator_index_candidate(
    inspection: PhysicalIndexRecommendationInspection,
) -> OperatorPhysicalIndexCandidateV3 {
    OperatorPhysicalIndexCandidateV3 {
        table_id: inspection.candidate.table_id.0,
        column_id: inspection.candidate.column_id.0,
        point_report_count: inspection.point_report_count,
        range_report_count: inspection.range_report_count,
        evidence: operator_evidence_summary(inspection.evidence),
        decision: operator_design_decision(inspection.decision),
    }
}

fn operator_columnar_candidate(
    inspection: PhysicalColumnarRecommendationInspection,
) -> OperatorPhysicalColumnarCandidateV3 {
    OperatorPhysicalColumnarCandidateV3 {
        table_id: inspection.candidate.table_id.0,
        columns: inspection
            .candidate
            .columns
            .into_iter()
            .map(|column| column.0)
            .collect(),
        evidence: operator_evidence_summary(inspection.evidence),
        decision: operator_design_decision(inspection.decision),
    }
}

const fn operator_evidence_summary(
    evidence: PhysicalDesignEvidenceSummary,
) -> OperatorPhysicalDesignEvidenceSummaryV3 {
    OperatorPhysicalDesignEvidenceSummaryV3 {
        report_count: evidence.report_count,
        distinct_query_shapes: evidence.distinct_query_shapes,
        total_actual_scan_work_units: evidence.total_actual_scan_work_units,
        total_rows_examined: evidence.total_rows_examined,
        overflowed: evidence.overflowed,
        incomplete: evidence.incomplete,
        truncated: evidence.truncated,
    }
}

const fn operator_design_decision(
    decision: PhysicalDesignCandidateDecision,
) -> OperatorPhysicalDesignDecisionV3 {
    match decision {
        PhysicalDesignCandidateDecision::Recommend => {
            OperatorPhysicalDesignDecisionV3::Recommend {}
        }
        PhysicalDesignCandidateDecision::NoAction(reason) => {
            OperatorPhysicalDesignDecisionV3::NoAction {
                reason: operator_no_action_reason(reason),
            }
        }
    }
}

const fn operator_no_action_reason(
    reason: PhysicalDesignNoActionReason,
) -> OperatorPhysicalDesignNoActionReasonV3 {
    match reason {
        PhysicalDesignNoActionReason::BelowMinimumReports => {
            OperatorPhysicalDesignNoActionReasonV3::BelowMinimumReports
        }
        PhysicalDesignNoActionReason::BelowMinimumShapeDiversity => {
            OperatorPhysicalDesignNoActionReasonV3::BelowMinimumShapeDiversity
        }
        PhysicalDesignNoActionReason::BelowMinimumActualWork => {
            OperatorPhysicalDesignNoActionReasonV3::BelowMinimumActualWork
        }
        PhysicalDesignNoActionReason::ExistingDesignCovers => {
            OperatorPhysicalDesignNoActionReasonV3::ExistingDesignCovers
        }
        PhysicalDesignNoActionReason::UnsupportedCurrentLayout => {
            OperatorPhysicalDesignNoActionReasonV3::UnsupportedCurrentLayout
        }
        PhysicalDesignNoActionReason::IncompleteEvidence => {
            OperatorPhysicalDesignNoActionReasonV3::IncompleteEvidence
        }
        PhysicalDesignNoActionReason::CurrentProjectionUnavailable => {
            OperatorPhysicalDesignNoActionReasonV3::CurrentProjectionUnavailable
        }
        PhysicalDesignNoActionReason::RecommendationLimitReached => {
            OperatorPhysicalDesignNoActionReasonV3::RecommendationLimitReached
        }
    }
}

const fn operator_physical_design_rotation(
    report: ServerPhysicalDesignRotationReport,
) -> OperatorPhysicalDesignRotationV3 {
    OperatorPhysicalDesignRotationV3 {
        previous_epoch: report.previous_epoch.0,
        new_epoch: report.new_epoch.0,
    }
}

fn control_remote_error(error: ServerAdaptiveControlError) -> OperatorRemoteErrorV3 {
    let code = match error {
        ServerAdaptiveControlError::AdaptiveNotEnabled => OperatorErrorCodeV3::AdaptiveNotEnabled,
        ServerAdaptiveControlError::DriverNotEnabled => OperatorErrorCodeV3::DriverNotEnabled,
        ServerAdaptiveControlError::SchedulerNotFaulted => OperatorErrorCodeV3::SchedulerNotFaulted,
        ServerAdaptiveControlError::EvidenceWindowChanged { .. } => {
            OperatorErrorCodeV3::EvidenceWindowChanged
        }
        ServerAdaptiveControlError::EvidenceRotation(
            AdaptiveEvidenceRotationError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV3::EvidenceWindowEpochExhausted,
        ServerAdaptiveControlError::ServerStopped => OperatorErrorCodeV3::ServerStopped,
    };
    let message = match code {
        OperatorErrorCodeV3::AdaptiveNotEnabled => "adaptive runtime is not enabled",
        OperatorErrorCodeV3::DriverNotEnabled => "adaptive driver is not enabled",
        OperatorErrorCodeV3::SchedulerNotFaulted => "adaptive scheduler is not faulted",
        OperatorErrorCodeV3::EvidenceWindowChanged => "adaptive evidence window changed",
        OperatorErrorCodeV3::EvidenceWindowEpochExhausted => {
            "adaptive evidence window epoch is exhausted"
        }
        OperatorErrorCodeV3::ServerStopped => "server adaptive control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV3 {
        code,
        message: message.into(),
    }
}

fn physical_design_remote_error(error: ServerPhysicalDesignControlError) -> OperatorRemoteErrorV3 {
    let code = match error {
        ServerPhysicalDesignControlError::PhysicalDesignNotEnabled => {
            OperatorErrorCodeV3::PhysicalDesignNotEnabled
        }
        ServerPhysicalDesignControlError::EvidenceEpochChanged { .. } => {
            OperatorErrorCodeV3::PhysicalDesignEvidenceEpochChanged
        }
        ServerPhysicalDesignControlError::EvidenceRotation(
            PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV3::PhysicalDesignEvidenceEpochExhausted,
        ServerPhysicalDesignControlError::EvidenceRotation(_) => OperatorErrorCodeV3::Internal,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
            OperatorErrorCodeV3::PhysicalDesignNoEvidence
        }
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
            ..
        }) => OperatorErrorCodeV3::PhysicalDesignStaleSchema,
        ServerPhysicalDesignControlError::Advisor(
            PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
        ) => OperatorErrorCodeV3::PhysicalDesignInconclusiveCapacity,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::Database(_)) => {
            OperatorErrorCodeV3::Internal
        }
        ServerPhysicalDesignControlError::Proposal(error) => match *error {
            PhysicalIndexDesignProposalError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
                OperatorErrorCodeV3::PhysicalDesignNoEvidence
            }
            PhysicalIndexDesignProposalError::Advisor(
                PhysicalDesignAdvisorError::StaleSchema { .. },
            ) => OperatorErrorCodeV3::PhysicalDesignStaleSchema,
            PhysicalIndexDesignProposalError::Advisor(
                PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
            ) => OperatorErrorCodeV3::PhysicalDesignInconclusiveCapacity,
            PhysicalIndexDesignProposalError::CandidateNotObserved(_) => {
                OperatorErrorCodeV3::PhysicalIndexCandidateNotObserved
            }
            PhysicalIndexDesignProposalError::CandidateNotRecommended { .. } => {
                OperatorErrorCodeV3::PhysicalIndexNotRecommended
            }
            PhysicalIndexDesignProposalError::GlobalVisibilityRequired
            | PhysicalIndexDesignProposalError::DurableCatalogRequired
            | PhysicalIndexDesignProposalError::Advisor(PhysicalDesignAdvisorError::Database(_))
            | PhysicalIndexDesignProposalError::Database(_) => {
                OperatorErrorCodeV3::PhysicalIndexApplyFailed
            }
        },
        ServerPhysicalDesignControlError::Apply(error) => match *error {
            PhysicalIndexDesignApplyError::EvidenceEpochChanged { .. } => {
                OperatorErrorCodeV3::PhysicalDesignEvidenceEpochChanged
            }
            PhysicalIndexDesignApplyError::CandidateNotObserved(_) => {
                OperatorErrorCodeV3::PhysicalIndexCandidateNotObserved
            }
            PhysicalIndexDesignApplyError::RecommendationNoLongerValid(_) => {
                OperatorErrorCodeV3::PhysicalIndexNotRecommended
            }
            PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
                OperatorErrorCodeV3::PhysicalDesignNoEvidence
            }
            PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
                ..
            }) => OperatorErrorCodeV3::PhysicalDesignStaleSchema,
            PhysicalIndexDesignApplyError::Advisor(
                PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
            ) => OperatorErrorCodeV3::PhysicalDesignInconclusiveCapacity,
            PhysicalIndexDesignApplyError::IndexNameConflict(_) => {
                OperatorErrorCodeV3::PhysicalIndexNameConflict
            }
            PhysicalIndexDesignApplyError::DatabaseIdentityChanged
            | PhysicalIndexDesignApplyError::StaleProposal(_)
            | PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::Database(_))
            | PhysicalIndexDesignApplyError::Database(_) => {
                OperatorErrorCodeV3::PhysicalIndexApplyFailed
            }
        },
        ServerPhysicalDesignControlError::PhysicalDesignRuntimeChanged
        | ServerPhysicalDesignControlError::ProposalRuntimeChanged => {
            OperatorErrorCodeV3::PhysicalDesignRuntimeChanged
        }
        ServerPhysicalDesignControlError::PhysicalIndexNameConflict(_) => {
            OperatorErrorCodeV3::PhysicalIndexNameConflict
        }
        ServerPhysicalDesignControlError::ServerStopped => OperatorErrorCodeV3::ServerStopped,
    };
    let message = match code {
        OperatorErrorCodeV3::PhysicalDesignNotEnabled => "physical-design advisor is not enabled",
        OperatorErrorCodeV3::PhysicalDesignEvidenceEpochChanged => {
            "physical-design evidence epoch changed"
        }
        OperatorErrorCodeV3::PhysicalDesignEvidenceEpochExhausted => {
            "physical-design evidence epoch is exhausted"
        }
        OperatorErrorCodeV3::PhysicalDesignNoEvidence => "physical-design evidence window is empty",
        OperatorErrorCodeV3::PhysicalDesignStaleSchema => {
            "physical-design evidence schema is stale"
        }
        OperatorErrorCodeV3::PhysicalDesignInconclusiveCapacity => {
            "physical-design evidence is capacity-truncated"
        }
        OperatorErrorCodeV3::PhysicalDesignRuntimeChanged => {
            "physical-design approval belongs to a previous daemon/operator runtime"
        }
        OperatorErrorCodeV3::PhysicalIndexCandidateNotObserved => {
            "physical-index candidate was not observed in current evidence"
        }
        OperatorErrorCodeV3::PhysicalIndexNotRecommended => {
            "physical-index candidate is not currently recommended"
        }
        OperatorErrorCodeV3::PhysicalIndexNameConflict => {
            "physical-index name is already bound to another target"
        }
        OperatorErrorCodeV3::PhysicalIndexApplyFailed => {
            "physical-index apply failed without creating a new index"
        }
        OperatorErrorCodeV3::ServerStopped => "server physical-design control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV3 {
        code,
        message: message.into(),
    }
}

fn response_too_large_error() -> OperatorRemoteErrorV3 {
    OperatorRemoteErrorV3 {
        code: OperatorErrorCodeV3::ResponseTooLarge,
        message: "operator response exceeds the NBOP payload limit".into(),
    }
}

fn protocol_remote_error(error: &OperatorProtocolError) -> OperatorRemoteErrorV3 {
    let (code, message) = match error {
        OperatorProtocolError::UnsupportedVersion(_) => (
            OperatorErrorCodeV3::UnsupportedProtocolVersion,
            "unsupported operator protocol version",
        ),
        OperatorProtocolError::RequestTooLarge(_) | OperatorProtocolError::PayloadTooLarge(_) => (
            OperatorErrorCodeV3::RequestTooLarge,
            "operator request is too large",
        ),
        OperatorProtocolError::TruncatedHeader | OperatorProtocolError::TruncatedPayload => (
            OperatorErrorCodeV3::MalformedRequest,
            "truncated operator request",
        ),
        OperatorProtocolError::WrongMagic => (
            OperatorErrorCodeV3::MalformedRequest,
            "invalid operator request magic",
        ),
        OperatorProtocolError::NonzeroReserved(_) => (
            OperatorErrorCodeV3::MalformedRequest,
            "operator reserved field is nonzero",
        ),
        OperatorProtocolError::InvalidJson(_) => (
            OperatorErrorCodeV3::MalformedRequest,
            "invalid operator request JSON",
        ),
        OperatorProtocolError::Io(_) => (
            OperatorErrorCodeV3::MalformedRequest,
            "operator request I/O failed",
        ),
    };
    OperatorRemoteErrorV3 {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::adaptive_driver::ServerAdaptiveControlRequest;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    #[cfg(unix)]
    fn socket_fixture(name: &str) -> (PathBuf, ServerOperatorConfig) {
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "netbadb-operator-{name}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let config = ServerOperatorConfig::new(
            directory.join("operator.sock"),
            Duration::from_millis(100),
            false,
        )
        .unwrap();
        (directory, config)
    }

    #[cfg(unix)]
    fn idle_plane(
        config: ServerOperatorConfig,
    ) -> Result<ServerOperatorPlane, ServerOperatorError> {
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let (server_shutdown, _server_shutdown_rx) = std::sync::mpsc::channel();
        ServerOperatorPlane::start(
            config,
            ServerAdaptiveControlHandle::new(control_tx),
            ServerPhysicalDesignControlHandle::new(design_tx),
            server_shutdown,
        )
    }

    #[test]
    fn frame_header_is_exact_big_endian_nbop_v3() {
        let request = OperatorRequestV3 {
            request_id: 42,
            operation: OperatorOperationV3::Status {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..6], &[0, 3]);
        assert_eq!(&bytes[6..8], &[0, 0]);
        assert_eq!(
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize,
            bytes.len() - OPERATOR_HEADER_BYTES
        );
        let decoded: OperatorRequestV3 = read_frame(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert!(matches!(decoded.operation, OperatorOperationV3::Status {}));
    }

    #[test]
    fn response_frame_and_request_id_echo_are_stable() {
        let response = OperatorResponseV3::Ok {
            request_id: 42,
            result: OperatorResultV3::SchedulerReset {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &response).unwrap();
        let payload = br#"{"outcome":"ok","request_id":42,"result":{"type":"scheduler_reset"}}"#;
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..8], &[0, 3, 0, 0]);
        assert_eq!(&bytes[8..12], &(payload.len() as u32).to_be_bytes());
        assert_eq!(&bytes[12..], payload);
    }

    #[test]
    fn physical_design_status_and_report_use_explicit_stable_dtos() {
        let status = operator_physical_design_status(
            ServerPhysicalDesignStatus {
                diagnostics: crate::ServerPhysicalDesignDiagnostics {
                    eligible_query_count: 1,
                    record_success_count: 2,
                    record_error_count: 3,
                    schema_rotation_count: 4,
                    capacity_rejection_count: 5,
                    incomplete_report_count: 6,
                    counter_overflowed: true,
                    last_record_outcome: Some(
                        PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection,
                    ),
                    last_record_error: Some(
                        PhysicalDesignEvidenceRecordError::OutOfOrderVisibility {
                            previous: netbadb_types::DatabaseCommitSeq(9),
                            received: netbadb_types::DatabaseCommitSeq(8),
                        },
                    ),
                },
                evidence: netbadb_core::PhysicalDesignEvidenceWindowInspection {
                    limits: netbadb_core::PhysicalDesignEvidenceLimits {
                        max_index_candidates: 10,
                        max_columnar_candidates: 11,
                        max_query_shapes_per_candidate: 12,
                        max_columnar_columns_per_candidate: 13,
                    },
                    epoch: netbadb_core::PhysicalDesignEvidenceEpoch(14),
                    schema_generation: Some(netbadb_types::SchemaGeneration(15)),
                    first_global_commit_seq: Some(netbadb_types::DatabaseCommitSeq(16)),
                    last_global_commit_seq: Some(netbadb_types::DatabaseCommitSeq(17)),
                    ordering_high_water: Some(netbadb_types::DatabaseCommitSeq(18)),
                    recorded_reports: 19,
                    index_candidate_count: 20,
                    columnar_candidate_count: 21,
                    capacity_rejections: 22,
                    discarded_incomplete_reports: 23,
                    overflowed: true,
                    incomplete: true,
                    truncated: true,
                },
            },
            false,
            None,
        );
        assert_eq!(status.diagnostics.eligible_query_count, 1);
        assert_eq!(
            status.diagnostics.last_record_outcome,
            Some(OperatorPhysicalDesignRecordOutcomeV3::SchemaRotatedWithCapacityRejection)
        );
        assert_eq!(
            status.diagnostics.last_record_error,
            Some(OperatorPhysicalDesignRecordErrorV3::OutOfOrderVisibility)
        );
        assert_eq!(
            status.evidence.limits.max_columnar_columns_per_candidate,
            13
        );
        assert_eq!(status.evidence.epoch, 14);
        assert!(!status.physical_index_apply.enabled);
        assert!(status.physical_index_apply.runtime_token.is_none());
        assert_eq!(status.evidence.ordering_high_water, Some(18));
        assert_eq!(status.evidence.discarded_incomplete_reports, 23);

        for (reason, expected) in [
            (
                PhysicalDesignNoActionReason::BelowMinimumReports,
                OperatorPhysicalDesignNoActionReasonV3::BelowMinimumReports,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumShapeDiversity,
                OperatorPhysicalDesignNoActionReasonV3::BelowMinimumShapeDiversity,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumActualWork,
                OperatorPhysicalDesignNoActionReasonV3::BelowMinimumActualWork,
            ),
            (
                PhysicalDesignNoActionReason::ExistingDesignCovers,
                OperatorPhysicalDesignNoActionReasonV3::ExistingDesignCovers,
            ),
            (
                PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
                OperatorPhysicalDesignNoActionReasonV3::UnsupportedCurrentLayout,
            ),
            (
                PhysicalDesignNoActionReason::IncompleteEvidence,
                OperatorPhysicalDesignNoActionReasonV3::IncompleteEvidence,
            ),
            (
                PhysicalDesignNoActionReason::CurrentProjectionUnavailable,
                OperatorPhysicalDesignNoActionReasonV3::CurrentProjectionUnavailable,
            ),
            (
                PhysicalDesignNoActionReason::RecommendationLimitReached,
                OperatorPhysicalDesignNoActionReasonV3::RecommendationLimitReached,
            ),
        ] {
            assert_eq!(operator_no_action_reason(reason), expected);
        }
    }

    #[test]
    fn runtime_token_encoding_is_exact_canonical_and_permission_precedes_forwarding() {
        let token = OperatorPhysicalDesignRuntimeToken([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        let encoded = token.encode();
        assert_eq!(encoded, "00112233445566778899aabbccddeeff");
        assert_eq!(
            OperatorPhysicalDesignRuntimeToken::parse(&encoded),
            Some(token)
        );
        for invalid in [
            "00112233445566778899AABBCCDDEEFF",
            "0x00112233445566778899aabbccddeeff",
            "00112233-4455-6677-8899-aabbccddeeff",
            "0011223344556677",
            "ABEiM0RVZneImaq7zN3u/w==",
        ] {
            assert!(OperatorPhysicalDesignRuntimeToken::parse(invalid).is_none());
        }

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV3 {
                request_id: 91,
                operation: OperatorOperationV3::ApplyPhysicalIndex {
                    expected_runtime_token: "invalid".into(),
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: String::new(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            false,
            None,
        );
        assert!(matches!(
            response,
            OperatorResponseV3::Error {
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::PhysicalIndexApplyNotEnabled,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV3 {
                request_id: 92,
                operation: OperatorOperationV3::ApplyPhysicalIndex {
                    expected_runtime_token: encoded,
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: String::new(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            true,
            Some(token),
        );
        assert!(matches!(
            response,
            OperatorResponseV3::Error {
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::InvalidIndexName,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV3 {
                request_id: 93,
                operation: OperatorOperationV3::ApplyPhysicalIndex {
                    expected_runtime_token: "00112233445566778899AABBCCDDEEFF".into(),
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: "users_name_idx".into(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            true,
            Some(token),
        );
        assert!(matches!(
            response,
            OperatorResponseV3::Error {
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::MalformedRequest,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn oversized_success_becomes_bounded_response_too_large_error() {
        let candidate = OperatorPhysicalIndexCandidateV3 {
            table_id: 1,
            column_id: 2,
            point_report_count: 3,
            range_report_count: 4,
            evidence: OperatorPhysicalDesignEvidenceSummaryV3 {
                report_count: 5,
                distinct_query_shapes: 6,
                total_actual_scan_work_units: 7,
                total_rows_examined: 8,
                overflowed: false,
                incomplete: false,
                truncated: false,
            },
            decision: OperatorPhysicalDesignDecisionV3::Recommend {},
        };
        let response = OperatorResponseV3::Ok {
            request_id: 77,
            result: OperatorResultV3::PhysicalDesignRecommendations {
                runtime_token: None,
                report: OperatorPhysicalDesignAdvisorReportV3 {
                    evidence_epoch: 1,
                    schema_generation: 2,
                    first_global_commit_seq: 3,
                    last_global_commit_seq: 4,
                    recorded_reports: 5,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: vec![candidate; 1_000],
                    columnar_candidates: Vec::new(),
                },
            },
        };
        let mut bytes = Vec::new();
        write_operator_response(&mut bytes, &response).unwrap();
        assert!(bytes.len() <= OPERATOR_HEADER_BYTES + MAX_OPERATOR_PAYLOAD_BYTES as usize);
        assert!(matches!(
            read_frame::<OperatorResponseV3>(&mut bytes.as_slice()).unwrap(),
            OperatorResponseV3::Error {
                request_id: 77,
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::ResponseTooLarge,
                    ..
                }
            }
        ));
    }

    #[test]
    fn reset_and_conditional_rotation_forward_typed_worker_results() {
        let (requests, controls) = std::sync::mpsc::channel();
        let control = ServerAdaptiveControlHandle::new(requests);
        let (design_requests, _design_controls) = std::sync::mpsc::channel();
        let design_control = ServerPhysicalDesignControlHandle::new(design_requests);
        let worker = std::thread::spawn(move || {
            match controls.recv().unwrap() {
                ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => {
                    reply.send(Ok(())).unwrap();
                }
                _ => panic!("unexpected first operator control request"),
            }
            match controls.recv().unwrap() {
                ServerAdaptiveControlRequest::RotateEvidenceIfWindow { expected, reply } => {
                    assert_eq!(expected, AdaptiveEvidenceWindowEpoch(7));
                    reply
                        .send(Err(ServerAdaptiveControlError::EvidenceWindowChanged {
                            expected,
                            actual: AdaptiveEvidenceWindowEpoch(8),
                        }))
                        .unwrap();
                }
                _ => panic!("unexpected second operator control request"),
            }
        });

        let reset = execute_operator_request(
            OperatorRequestV3 {
                request_id: 11,
                operation: OperatorOperationV3::ResetFaultedScheduler {},
            },
            &control,
            &design_control,
            false,
            None,
        );
        assert!(matches!(
            reset,
            OperatorResponseV3::Ok {
                request_id: 11,
                result: OperatorResultV3::SchedulerReset {}
            }
        ));

        let stale = execute_operator_request(
            OperatorRequestV3 {
                request_id: 12,
                operation: OperatorOperationV3::RotateEvidence {
                    expected_window_epoch: 7,
                },
            },
            &control,
            &design_control,
            false,
            None,
        );
        assert!(matches!(
            stale,
            OperatorResponseV3::Error {
                request_id: 12,
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::EvidenceWindowChanged,
                    ..
                }
            }
        ));
        worker.join().unwrap();
    }

    #[test]
    fn codec_rejects_header_and_payload_violations() {
        let mut wrong_magic = [0_u8; OPERATOR_HEADER_BYTES];
        wrong_magic[..4].copy_from_slice(b"NOPE");
        wrong_magic[4..6].copy_from_slice(&3_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut wrong_magic.as_slice()),
            Err(OperatorProtocolError::WrongMagic)
        ));

        let mut wrong_version = [0_u8; OPERATOR_HEADER_BYTES];
        wrong_version[..4].copy_from_slice(b"NBOP");
        wrong_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut wrong_version.as_slice()),
            Err(OperatorProtocolError::UnsupportedVersion(2))
        ));

        let mut reserved = [0_u8; OPERATOR_HEADER_BYTES];
        reserved[..4].copy_from_slice(b"NBOP");
        reserved[4..6].copy_from_slice(&3_u16.to_be_bytes());
        reserved[6..8].copy_from_slice(&1_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut reserved.as_slice()),
            Err(OperatorProtocolError::NonzeroReserved(1))
        ));

        let mut oversized = [0_u8; OPERATOR_HEADER_BYTES];
        oversized[..4].copy_from_slice(b"NBOP");
        oversized[4..6].copy_from_slice(&3_u16.to_be_bytes());
        oversized[8..12].copy_from_slice(&(MAX_OPERATOR_PAYLOAD_BYTES + 1).to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut oversized.as_slice()),
            Err(OperatorProtocolError::RequestTooLarge(_))
        ));
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut b"NB".as_slice()),
            Err(OperatorProtocolError::TruncatedHeader)
        ));

        let truncated_payload = b"NBOP\0\x03\0\0\0\0\0\x02{".to_vec();
        assert!(matches!(
            read_frame::<OperatorRequestV3>(&mut truncated_payload.as_slice()),
            Err(OperatorProtocolError::TruncatedPayload)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn v2_client_receives_explicit_unsupported_protocol_version() {
        use std::os::unix::net::UnixStream;

        let (mut client, mut server) = UnixStream::pair().unwrap();
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            assert!(matches!(
                serve_operator_connection(
                    &mut server,
                    &ServerAdaptiveControlHandle::new(adaptive_tx),
                    &ServerPhysicalDesignControlHandle::new(design_tx),
                    false,
                    None,
                ),
                Err(OperatorProtocolError::UnsupportedVersion(2))
            ));
        });
        let mut v2_header = [0_u8; OPERATOR_HEADER_BYTES];
        v2_header[..4].copy_from_slice(b"NBOP");
        v2_header[4..6].copy_from_slice(&2_u16.to_be_bytes());
        client.write_all(&v2_header).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV3>(&mut client).unwrap(),
            OperatorResponseV3::Error {
                request_id: 0,
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::UnsupportedProtocolVersion,
                    ..
                }
            }
        ));
        worker.join().unwrap();
    }

    #[test]
    fn strict_request_json_rejects_unknown_fields_and_operations() {
        fn framed(payload: &[u8]) -> Vec<u8> {
            let mut bytes = Vec::from(b"NBOP\0\x03\0\0".as_slice());
            bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            bytes.extend_from_slice(payload);
            bytes
        }
        for payload in [
            br#"{"request_id":1,"operation":{"type":"status","extra":true}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"unknown"}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"apply_physical_index","index_name":"idx"}}"#
                .as_slice(),
            &[0xff][..],
        ] {
            assert!(matches!(
                read_frame::<OperatorRequestV3>(&mut framed(payload).as_slice()),
                Err(OperatorProtocolError::InvalidJson(_))
            ));
        }
    }

    #[test]
    fn armed_failure_notification_reports_abnormal_listener_exit() {
        let (sender, receiver) = std::sync::mpsc::channel();
        drop(OperatorFailureNotification::new(sender));
        assert_eq!(receiver.recv().unwrap(), ());

        let (sender, receiver) = std::sync::mpsc::channel();
        let mut notification = OperatorFailureNotification::new(sender);
        notification.disarm();
        drop(notification);
        assert!(receiver.recv().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn listener_forwards_only_one_active_control_request() {
        use std::os::unix::net::UnixStream;
        use std::sync::mpsc::TryRecvError;

        fn reset_frame(request_id: u64) -> Vec<u8> {
            let mut bytes = Vec::new();
            write_frame(
                &mut bytes,
                &OperatorRequestV3 {
                    request_id,
                    operation: OperatorOperationV3::ResetFaultedScheduler {},
                },
            )
            .unwrap();
            bytes
        }

        let (directory, mut config) = socket_fixture("serial");
        config.io_timeout = Duration::from_secs(1);
        let path = config.unix_socket().to_path_buf();
        let (control_tx, control_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let (failure_tx, _failure_rx) = std::sync::mpsc::channel();
        let plane = ServerOperatorPlane::start(
            config,
            ServerAdaptiveControlHandle::new(control_tx),
            ServerPhysicalDesignControlHandle::new(design_tx),
            failure_tx,
        )
        .unwrap();

        let mut first = UnixStream::connect(&path).unwrap();
        let mut second = UnixStream::connect(&path).unwrap();
        first.write_all(&reset_frame(1)).unwrap();
        second.write_all(&reset_frame(2)).unwrap();

        let first_reply = match control_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => reply,
            _ => panic!("unexpected first operator control request"),
        };
        assert!(matches!(control_rx.try_recv(), Err(TryRecvError::Empty)));
        first_reply.send(Ok(())).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV3>(&mut first).unwrap(),
            OperatorResponseV3::Ok { request_id: 1, .. }
        ));

        let second_reply = match control_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => reply,
            _ => panic!("unexpected second operator control request"),
        };
        second_reply
            .send(Err(ServerAdaptiveControlError::SchedulerNotFaulted))
            .unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV3>(&mut second).unwrap(),
            OperatorResponseV3::Error {
                request_id: 2,
                error: OperatorRemoteErrorV3 {
                    code: OperatorErrorCodeV3::SchedulerNotFaulted,
                    ..
                }
            }
        ));

        plane.shutdown().unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_with_an_incomplete_request_is_bounded_by_io_timeout() {
        use std::os::unix::net::UnixStream;

        let (directory, mut config) = socket_fixture("partial");
        config.io_timeout = Duration::from_millis(20);
        let path = config.unix_socket().to_path_buf();
        let plane = idle_plane(config).unwrap();
        let _incomplete = UnixStream::connect(&path).unwrap();
        plane.shutdown().unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn socket_mode_cleanup_and_replacement_identity_are_exact() {
        use std::os::unix::fs::PermissionsExt;

        let (directory, config) = socket_fixture("lifecycle");
        let path = config.unix_socket().to_path_buf();
        let plane = idle_plane(config.clone()).unwrap();
        assert!(!plane.is_finished());
        let mode = std::fs::symlink_metadata(&path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        plane.shutdown().unwrap();
        assert!(!path.exists());

        let plane = idle_plane(config).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(matches!(
            plane.shutdown(),
            Err(ServerOperatorError::SocketPathReplaced(actual)) if actual == path
        ));
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn every_existing_path_kind_and_stale_socket_is_preserved() {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;

        for kind in ["file", "symlink", "directory", "socket"] {
            let (directory, config) = socket_fixture(kind);
            let path = config.unix_socket().to_path_buf();
            match kind {
                "file" => std::fs::write(&path, b"owned elsewhere").unwrap(),
                "symlink" => symlink(directory.join("missing-target"), &path).unwrap(),
                "directory" => std::fs::create_dir(&path).unwrap(),
                "socket" => {
                    let stale = UnixListener::bind(&path).unwrap();
                    drop(stale);
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                idle_plane(config),
                Err(ServerOperatorError::PathExists(actual)) if actual == path
            ));
            assert!(std::fs::symlink_metadata(&path).is_ok());
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
