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
    PhysicalDesignEvidenceSummary, PhysicalDesignNoActionReason,
    PhysicalIndexRecommendationInspection,
};
use serde::{Deserialize, Serialize};

use crate::{
    ServerAdaptiveControlError, ServerAdaptiveControlHandle, ServerAdaptiveMode,
    ServerAdaptiveStatus, ServerPhysicalDesignControlError, ServerPhysicalDesignControlHandle,
    ServerPhysicalDesignRotationReport, ServerPhysicalDesignStatus,
};

pub const OPERATOR_PROTOCOL_VERSION: u16 = 2;
pub const MAX_OPERATOR_PAYLOAD_BYTES: u32 = 64 * 1024;

const OPERATOR_MAGIC: [u8; 4] = *b"NBOP";
const OPERATOR_HEADER_BYTES: usize = 12;

/// Runtime-ready local operator configuration resolved from the deployment
/// manifest. The socket path is absolute and its parent already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOperatorConfig {
    unix_socket: PathBuf,
    io_timeout: Duration,
}

impl ServerOperatorConfig {
    pub(crate) fn new(
        unix_socket: PathBuf,
        io_timeout: Duration,
    ) -> Result<Self, ServerOperatorConfigError> {
        if io_timeout.is_zero() {
            return Err(ServerOperatorConfigError::ZeroIoTimeout);
        }
        Ok(Self {
            unix_socket,
            io_timeout,
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
pub enum OperatorAdaptiveModeV2 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV2 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV2 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV2 {
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
pub enum OperatorSchedulerDelayClassV2 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV2 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV2 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV2 {
    Open {
        delay_class: OperatorSchedulerDelayClassV2,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV2,
    },
    Faulted {
        fault: OperatorSchedulerFaultV2,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV2 {
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
pub struct OperatorFeedbackStatusV2 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV2>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV2>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV2 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV2,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV2>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV2 {
    pub mode: OperatorAdaptiveModeV2,
    pub feedback: OperatorFeedbackStatusV2,
    pub driver: Option<OperatorDriverStatusV2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV2 {
    pub adaptive: Option<OperatorAdaptiveStatusV2>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV2 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV2 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV2 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV2>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV2 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV2 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV2,
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
pub struct OperatorPhysicalDesignStatusV2 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV2,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV2 {
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
pub enum OperatorPhysicalDesignNoActionReasonV2 {
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
pub enum OperatorPhysicalDesignDecisionV2 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV2,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV2 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV2,
    pub decision: OperatorPhysicalDesignDecisionV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV2 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV2,
    pub decision: OperatorPhysicalDesignDecisionV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV2 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV2>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV2 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV2 {
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
pub enum OperatorErrorCodeV2 {
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
    ResponseTooLarge,
    ServerStopped,
    MalformedRequest,
    UnsupportedProtocolVersion,
    RequestTooLarge,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorRemoteErrorV2 {
    pub code: OperatorErrorCodeV2,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorRequestV2 {
    request_id: u64,
    operation: OperatorOperationV2,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorOperationV2 {
    Status {},
    RotateEvidence { expected_window_epoch: u64 },
    ResetFaultedScheduler {},
    PhysicalDesignRecommendations {},
    RotatePhysicalDesignEvidence { expected_evidence_epoch: u64 },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResponseV2 {
    Ok {
        request_id: u64,
        result: OperatorResultV2,
    },
    Error {
        request_id: u64,
        error: OperatorRemoteErrorV2,
    },
}

impl OperatorResponseV2 {
    const fn request_id(&self) -> u64 {
        match self {
            Self::Ok { request_id, .. } | Self::Error { request_id, .. } => *request_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResultV2 {
    Status {
        status: Box<OperatorStatusV2>,
    },
    EvidenceRotated {
        rotation: OperatorEvidenceRotationV2,
    },
    SchedulerReset {},
    PhysicalDesignRecommendations {
        report: OperatorPhysicalDesignAdvisorReportV2,
    },
    PhysicalDesignEvidenceRotated {
        rotation: OperatorPhysicalDesignRotationV2,
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
    Remote(OperatorRemoteErrorV2),
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

    pub fn status(&self) -> Result<OperatorStatusV2, OperatorClientError> {
        match self.exchange(OperatorOperationV2::Status {})? {
            OperatorResultV2::Status { status } => Ok(*status),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_evidence(
        &self,
        expected_window_epoch: u64,
    ) -> Result<OperatorEvidenceRotationV2, OperatorClientError> {
        match self.exchange(OperatorOperationV2::RotateEvidence {
            expected_window_epoch,
        })? {
            OperatorResultV2::EvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn reset_faulted_scheduler(&self) -> Result<(), OperatorClientError> {
        match self.exchange(OperatorOperationV2::ResetFaultedScheduler {})? {
            OperatorResultV2::SchedulerReset {} => Ok(()),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn physical_design_recommendations(
        &self,
    ) -> Result<OperatorPhysicalDesignAdvisorReportV2, OperatorClientError> {
        match self.exchange(OperatorOperationV2::PhysicalDesignRecommendations {})? {
            OperatorResultV2::PhysicalDesignRecommendations { report } => Ok(report),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_physical_design_evidence(
        &self,
        expected_evidence_epoch: u64,
    ) -> Result<OperatorPhysicalDesignRotationV2, OperatorClientError> {
        match self.exchange(OperatorOperationV2::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        })? {
            OperatorResultV2::PhysicalDesignEvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    #[cfg(unix)]
    fn exchange(
        &self,
        operation: OperatorOperationV2,
    ) -> Result<OperatorResultV2, OperatorClientError> {
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
            &OperatorRequestV2 {
                request_id,
                operation,
            },
        )
        .map_err(OperatorClientError::Protocol)?;
        let response: OperatorResponseV2 =
            read_frame(&mut stream).map_err(OperatorClientError::Protocol)?;
        match response {
            OperatorResponseV2::Ok {
                request_id: received,
                result,
            } => {
                verify_request_id(request_id, received)?;
                Ok(result)
            }
            OperatorResponseV2::Error {
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
        _operation: OperatorOperationV2,
    ) -> Result<OperatorResultV2, OperatorClientError> {
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
) -> Result<(), OperatorProtocolError> {
    let request = match read_frame::<OperatorRequestV2>(stream) {
        Ok(request) => request,
        Err(error) => {
            let response = OperatorResponseV2::Error {
                request_id: 0,
                error: protocol_remote_error(&error),
            };
            let _ = write_frame(stream, &response);
            return Err(error);
        }
    };
    let response = execute_operator_request(request, adaptive_control, physical_design_control);
    write_operator_response(stream, &response)
}

fn write_operator_response(
    writer: &mut impl Write,
    response: &OperatorResponseV2,
) -> Result<(), OperatorProtocolError> {
    let request_id = response.request_id();
    match write_frame(writer, response) {
        Err(OperatorProtocolError::PayloadTooLarge(_)) => write_frame(
            writer,
            &OperatorResponseV2::Error {
                request_id,
                error: response_too_large_error(),
            },
        ),
        result => result,
    }
}

fn execute_operator_request(
    request: OperatorRequestV2,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
) -> OperatorResponseV2 {
    let result = match request.operation {
        OperatorOperationV2::Status {} => {
            operator_status(adaptive_control, physical_design_control).map(|status| {
                OperatorResultV2::Status {
                    status: Box::new(status),
                }
            })
        }
        OperatorOperationV2::RotateEvidence {
            expected_window_epoch,
        } => adaptive_control
            .rotate_evidence_if_window(AdaptiveEvidenceWindowEpoch(expected_window_epoch))
            .map(operator_rotation)
            .map(|rotation| OperatorResultV2::EvidenceRotated { rotation })
            .map_err(control_remote_error),
        OperatorOperationV2::ResetFaultedScheduler {} => adaptive_control
            .reset_faulted_scheduler()
            .map(|()| OperatorResultV2::SchedulerReset {})
            .map_err(control_remote_error),
        OperatorOperationV2::PhysicalDesignRecommendations {} => physical_design_control
            .recommendations()
            .map(operator_physical_design_report)
            .map(|report| OperatorResultV2::PhysicalDesignRecommendations { report })
            .map_err(physical_design_remote_error),
        OperatorOperationV2::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        } => physical_design_control
            .rotate_evidence_if_epoch(netbadb_core::PhysicalDesignEvidenceEpoch(
                expected_evidence_epoch,
            ))
            .map(operator_physical_design_rotation)
            .map(|rotation| OperatorResultV2::PhysicalDesignEvidenceRotated { rotation })
            .map_err(physical_design_remote_error),
    };
    match result {
        Ok(result) => OperatorResponseV2::Ok {
            request_id: request.request_id,
            result,
        },
        Err(error) => OperatorResponseV2::Error {
            request_id: request.request_id,
            error,
        },
    }
}

fn operator_status(
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
) -> Result<OperatorStatusV2, OperatorRemoteErrorV2> {
    let adaptive = match adaptive_control.status() {
        Ok(status) if status.mode == ServerAdaptiveMode::Disabled => None,
        Ok(status) => Some(operator_adaptive_status(status).map_err(control_remote_error)?),
        Err(ServerAdaptiveControlError::AdaptiveNotEnabled) => None,
        Err(error) => return Err(control_remote_error(error)),
    };
    let physical_design = match physical_design_control.status() {
        Ok(status) => Some(operator_physical_design_status(status)),
        Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled) => None,
        Err(error) => return Err(physical_design_remote_error(error)),
    };
    if adaptive.is_none() && physical_design.is_none() {
        return Err(OperatorRemoteErrorV2 {
            code: OperatorErrorCodeV2::Internal,
            message: "operator plane has no managed runtime".into(),
        });
    }
    Ok(OperatorStatusV2 {
        adaptive,
        physical_design,
    })
}

fn operator_adaptive_status(
    status: ServerAdaptiveStatus,
) -> Result<OperatorAdaptiveStatusV2, ServerAdaptiveControlError> {
    let mode = match status.mode {
        ServerAdaptiveMode::FeedbackOnly => OperatorAdaptiveModeV2::FeedbackOnly,
        ServerAdaptiveMode::Driven => OperatorAdaptiveModeV2::Driven,
        ServerAdaptiveMode::Disabled => return Err(ServerAdaptiveControlError::AdaptiveNotEnabled),
    };
    let feedback = status
        .feedback
        .ok_or(ServerAdaptiveControlError::AdaptiveNotEnabled)?;
    let progress = feedback.evidence_progress;
    let feedback = OperatorFeedbackStatusV2 {
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
            AdaptiveEvidencePoolHealth::Healthy => OperatorEvidencePoolHealthV2::Healthy,
            AdaptiveEvidencePoolHealth::RotationRecommended => {
                OperatorEvidencePoolHealthV2::RotationRecommended
            }
        },
    };
    let driver = status.driver.map(|driver| OperatorDriverStatusV2 {
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
    Ok(OperatorAdaptiveStatusV2 {
        mode,
        feedback,
        driver,
    })
}

fn record_outcome(outcome: AdaptiveEvidenceRecordOutcome) -> OperatorEvidenceRecordOutcomeV2 {
    match outcome {
        AdaptiveEvidenceRecordOutcome::Recorded => OperatorEvidenceRecordOutcomeV2::Recorded,
        AdaptiveEvidenceRecordOutcome::SchemaRotated => {
            OperatorEvidenceRecordOutcomeV2::SchemaRotated
        }
        AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV2::RecordedWithCapacityRejection
        }
        AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV2::SchemaRotatedWithCapacityRejection
        }
    }
}

fn record_error(error: AdaptiveEvidenceRecordError) -> OperatorEvidenceRecordErrorV2 {
    match error {
        AdaptiveEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorEvidenceRecordErrorV2::GlobalVisibilityRequired
        }
        AdaptiveEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorEvidenceRecordErrorV2::StaleSchemaEvidence
        }
        AdaptiveEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorEvidenceRecordErrorV2::OutOfOrderVisibility
        }
        AdaptiveEvidenceRecordError::StaleTargetGenerationEvidence { .. } => {
            OperatorEvidenceRecordErrorV2::StaleTargetGenerationEvidence
        }
        AdaptiveEvidenceRecordError::StaleTargetIdentityEvidence { .. } => {
            OperatorEvidenceRecordErrorV2::StaleTargetIdentityEvidence
        }
        AdaptiveEvidenceRecordError::RetiredTargetEvidence { .. } => {
            OperatorEvidenceRecordErrorV2::RetiredTargetEvidence
        }
        AdaptiveEvidenceRecordError::StaleCalibrationEpochEvidence { .. } => {
            OperatorEvidenceRecordErrorV2::StaleCalibrationEpochEvidence
        }
        AdaptiveEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorEvidenceRecordErrorV2::EvidenceWindowEpochExhausted
        }
    }
}

fn scheduler_gate(gate: AutomaticSchedulerGate) -> OperatorSchedulerGateV2 {
    match gate {
        AutomaticSchedulerGate::Open { delay } => OperatorSchedulerGateV2::Open {
            delay_class: match delay {
                AutomaticSchedulerDelayClass::Normal => OperatorSchedulerDelayClassV2::Normal,
                AutomaticSchedulerDelayClass::Idle => OperatorSchedulerDelayClassV2::Idle,
                AutomaticSchedulerDelayClass::NoProgress => {
                    OperatorSchedulerDelayClassV2::NoProgress
                }
            },
        },
        AutomaticSchedulerGate::AwaitingTrialProgress { evidence } => {
            OperatorSchedulerGateV2::AwaitingTrialProgress {
                window_epoch: evidence.window_epoch.0,
                schema_generation: evidence.schema_generation.map(|generation| generation.0),
                recorded_reports: evidence.recorded_reports,
            }
        }
        AutomaticSchedulerGate::AwaitingEvidenceRenewal {
            blocked_window_epoch,
            recommendation,
        } => OperatorSchedulerGateV2::AwaitingEvidenceRenewal {
            blocked_window_epoch: blocked_window_epoch.0,
            renewal_reason: match recommendation.reason {
                AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged => {
                    OperatorEvidenceRenewalReasonV2::ColumnarPhysicalStateChanged
                }
                AutomaticEvidenceRenewalReason::ColumnarEligibilityChanged => {
                    OperatorEvidenceRenewalReasonV2::ColumnarEligibilityChanged
                }
                AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged => {
                    OperatorEvidenceRenewalReasonV2::AuthoritativeLsmLayoutChanged
                }
            },
        },
        AutomaticSchedulerGate::Faulted(fault) => OperatorSchedulerGateV2::Faulted {
            fault: match fault {
                AutomaticSchedulerFault::MaintenanceEnvelopeExceeded => {
                    OperatorSchedulerFaultV2::MaintenanceEnvelopeExceeded
                }
                AutomaticSchedulerFault::StepFailed => OperatorSchedulerFaultV2::StepFailed,
                AutomaticSchedulerFault::ConsumptionOverflow => {
                    OperatorSchedulerFaultV2::ConsumptionOverflow
                }
            },
        },
    }
}

fn orchestration_stop_reason(
    reason: AutomaticOrchestrationStopReason,
) -> OperatorOrchestrationStopReasonV2 {
    match reason {
        AutomaticOrchestrationStopReason::NoReadyWork => {
            OperatorOrchestrationStopReasonV2::NoReadyWork
        }
        AutomaticOrchestrationStopReason::StepLimitReached => {
            OperatorOrchestrationStopReasonV2::StepLimitReached
        }
        AutomaticOrchestrationStopReason::ActiveTrial(_) => {
            OperatorOrchestrationStopReasonV2::ActiveTrial
        }
        AutomaticOrchestrationStopReason::TrialBoundaryResolved => {
            OperatorOrchestrationStopReasonV2::TrialBoundaryResolved
        }
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(_) => {
            OperatorOrchestrationStopReasonV2::EvidenceRenewalRecommended
        }
        AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress => {
            OperatorOrchestrationStopReasonV2::SelectedCandidateDidNotProgress
        }
        AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded { .. } => {
            OperatorOrchestrationStopReasonV2::MaintenanceEnvelopeExceeded
        }
    }
}

fn operator_rotation(report: AdaptiveEvidenceRotationReport) -> OperatorEvidenceRotationV2 {
    OperatorEvidenceRotationV2 {
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
) -> OperatorPhysicalDesignStatusV2 {
    let diagnostics = status.diagnostics;
    let evidence = status.evidence;
    OperatorPhysicalDesignStatusV2 {
        diagnostics: OperatorPhysicalDesignDiagnosticsV2 {
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
        evidence: OperatorPhysicalDesignEvidenceStatusV2 {
            limits: OperatorPhysicalDesignEvidenceLimitsV2 {
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
    }
}

const fn physical_design_record_outcome(
    outcome: PhysicalDesignEvidenceRecordOutcome,
) -> OperatorPhysicalDesignRecordOutcomeV2 {
    match outcome {
        PhysicalDesignEvidenceRecordOutcome::Recorded => {
            OperatorPhysicalDesignRecordOutcomeV2::Recorded
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotated => {
            OperatorPhysicalDesignRecordOutcomeV2::SchemaRotated
        }
        PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV2::RecordedWithCapacityRejection
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV2::SchemaRotatedWithCapacityRejection
        }
    }
}

const fn physical_design_record_error(
    error: PhysicalDesignEvidenceRecordError,
) -> OperatorPhysicalDesignRecordErrorV2 {
    match error {
        PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorPhysicalDesignRecordErrorV2::GlobalVisibilityRequired
        }
        PhysicalDesignEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorPhysicalDesignRecordErrorV2::StaleSchemaEvidence
        }
        PhysicalDesignEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorPhysicalDesignRecordErrorV2::OutOfOrderVisibility
        }
        PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorPhysicalDesignRecordErrorV2::EvidenceWindowEpochExhausted
        }
    }
}

fn operator_physical_design_report(
    report: PhysicalDesignAdvisorReport,
) -> OperatorPhysicalDesignAdvisorReportV2 {
    OperatorPhysicalDesignAdvisorReportV2 {
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
) -> OperatorPhysicalIndexCandidateV2 {
    OperatorPhysicalIndexCandidateV2 {
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
) -> OperatorPhysicalColumnarCandidateV2 {
    OperatorPhysicalColumnarCandidateV2 {
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
) -> OperatorPhysicalDesignEvidenceSummaryV2 {
    OperatorPhysicalDesignEvidenceSummaryV2 {
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
) -> OperatorPhysicalDesignDecisionV2 {
    match decision {
        PhysicalDesignCandidateDecision::Recommend => {
            OperatorPhysicalDesignDecisionV2::Recommend {}
        }
        PhysicalDesignCandidateDecision::NoAction(reason) => {
            OperatorPhysicalDesignDecisionV2::NoAction {
                reason: operator_no_action_reason(reason),
            }
        }
    }
}

const fn operator_no_action_reason(
    reason: PhysicalDesignNoActionReason,
) -> OperatorPhysicalDesignNoActionReasonV2 {
    match reason {
        PhysicalDesignNoActionReason::BelowMinimumReports => {
            OperatorPhysicalDesignNoActionReasonV2::BelowMinimumReports
        }
        PhysicalDesignNoActionReason::BelowMinimumShapeDiversity => {
            OperatorPhysicalDesignNoActionReasonV2::BelowMinimumShapeDiversity
        }
        PhysicalDesignNoActionReason::BelowMinimumActualWork => {
            OperatorPhysicalDesignNoActionReasonV2::BelowMinimumActualWork
        }
        PhysicalDesignNoActionReason::ExistingDesignCovers => {
            OperatorPhysicalDesignNoActionReasonV2::ExistingDesignCovers
        }
        PhysicalDesignNoActionReason::UnsupportedCurrentLayout => {
            OperatorPhysicalDesignNoActionReasonV2::UnsupportedCurrentLayout
        }
        PhysicalDesignNoActionReason::IncompleteEvidence => {
            OperatorPhysicalDesignNoActionReasonV2::IncompleteEvidence
        }
        PhysicalDesignNoActionReason::CurrentProjectionUnavailable => {
            OperatorPhysicalDesignNoActionReasonV2::CurrentProjectionUnavailable
        }
        PhysicalDesignNoActionReason::RecommendationLimitReached => {
            OperatorPhysicalDesignNoActionReasonV2::RecommendationLimitReached
        }
    }
}

const fn operator_physical_design_rotation(
    report: ServerPhysicalDesignRotationReport,
) -> OperatorPhysicalDesignRotationV2 {
    OperatorPhysicalDesignRotationV2 {
        previous_epoch: report.previous_epoch.0,
        new_epoch: report.new_epoch.0,
    }
}

fn control_remote_error(error: ServerAdaptiveControlError) -> OperatorRemoteErrorV2 {
    let code = match error {
        ServerAdaptiveControlError::AdaptiveNotEnabled => OperatorErrorCodeV2::AdaptiveNotEnabled,
        ServerAdaptiveControlError::DriverNotEnabled => OperatorErrorCodeV2::DriverNotEnabled,
        ServerAdaptiveControlError::SchedulerNotFaulted => OperatorErrorCodeV2::SchedulerNotFaulted,
        ServerAdaptiveControlError::EvidenceWindowChanged { .. } => {
            OperatorErrorCodeV2::EvidenceWindowChanged
        }
        ServerAdaptiveControlError::EvidenceRotation(
            AdaptiveEvidenceRotationError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV2::EvidenceWindowEpochExhausted,
        ServerAdaptiveControlError::ServerStopped => OperatorErrorCodeV2::ServerStopped,
    };
    let message = match code {
        OperatorErrorCodeV2::AdaptiveNotEnabled => "adaptive runtime is not enabled",
        OperatorErrorCodeV2::DriverNotEnabled => "adaptive driver is not enabled",
        OperatorErrorCodeV2::SchedulerNotFaulted => "adaptive scheduler is not faulted",
        OperatorErrorCodeV2::EvidenceWindowChanged => "adaptive evidence window changed",
        OperatorErrorCodeV2::EvidenceWindowEpochExhausted => {
            "adaptive evidence window epoch is exhausted"
        }
        OperatorErrorCodeV2::ServerStopped => "server adaptive control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV2 {
        code,
        message: message.into(),
    }
}

fn physical_design_remote_error(error: ServerPhysicalDesignControlError) -> OperatorRemoteErrorV2 {
    let code = match error {
        ServerPhysicalDesignControlError::PhysicalDesignNotEnabled => {
            OperatorErrorCodeV2::PhysicalDesignNotEnabled
        }
        ServerPhysicalDesignControlError::EvidenceEpochChanged { .. } => {
            OperatorErrorCodeV2::PhysicalDesignEvidenceEpochChanged
        }
        ServerPhysicalDesignControlError::EvidenceRotation(
            PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV2::PhysicalDesignEvidenceEpochExhausted,
        ServerPhysicalDesignControlError::EvidenceRotation(_) => OperatorErrorCodeV2::Internal,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
            OperatorErrorCodeV2::PhysicalDesignNoEvidence
        }
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
            ..
        }) => OperatorErrorCodeV2::PhysicalDesignStaleSchema,
        ServerPhysicalDesignControlError::Advisor(
            PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
        ) => OperatorErrorCodeV2::PhysicalDesignInconclusiveCapacity,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::Database(_)) => {
            OperatorErrorCodeV2::Internal
        }
        ServerPhysicalDesignControlError::ServerStopped => OperatorErrorCodeV2::ServerStopped,
    };
    let message = match code {
        OperatorErrorCodeV2::PhysicalDesignNotEnabled => "physical-design advisor is not enabled",
        OperatorErrorCodeV2::PhysicalDesignEvidenceEpochChanged => {
            "physical-design evidence epoch changed"
        }
        OperatorErrorCodeV2::PhysicalDesignEvidenceEpochExhausted => {
            "physical-design evidence epoch is exhausted"
        }
        OperatorErrorCodeV2::PhysicalDesignNoEvidence => "physical-design evidence window is empty",
        OperatorErrorCodeV2::PhysicalDesignStaleSchema => {
            "physical-design evidence schema is stale"
        }
        OperatorErrorCodeV2::PhysicalDesignInconclusiveCapacity => {
            "physical-design evidence is capacity-truncated"
        }
        OperatorErrorCodeV2::ServerStopped => "server physical-design control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV2 {
        code,
        message: message.into(),
    }
}

fn response_too_large_error() -> OperatorRemoteErrorV2 {
    OperatorRemoteErrorV2 {
        code: OperatorErrorCodeV2::ResponseTooLarge,
        message: "operator response exceeds the NBOP payload limit".into(),
    }
}

fn protocol_remote_error(error: &OperatorProtocolError) -> OperatorRemoteErrorV2 {
    let (code, message) = match error {
        OperatorProtocolError::UnsupportedVersion(_) => (
            OperatorErrorCodeV2::UnsupportedProtocolVersion,
            "unsupported operator protocol version",
        ),
        OperatorProtocolError::RequestTooLarge(_) | OperatorProtocolError::PayloadTooLarge(_) => (
            OperatorErrorCodeV2::RequestTooLarge,
            "operator request is too large",
        ),
        OperatorProtocolError::TruncatedHeader | OperatorProtocolError::TruncatedPayload => (
            OperatorErrorCodeV2::MalformedRequest,
            "truncated operator request",
        ),
        OperatorProtocolError::WrongMagic => (
            OperatorErrorCodeV2::MalformedRequest,
            "invalid operator request magic",
        ),
        OperatorProtocolError::NonzeroReserved(_) => (
            OperatorErrorCodeV2::MalformedRequest,
            "operator reserved field is nonzero",
        ),
        OperatorProtocolError::InvalidJson(_) => (
            OperatorErrorCodeV2::MalformedRequest,
            "invalid operator request JSON",
        ),
        OperatorProtocolError::Io(_) => (
            OperatorErrorCodeV2::MalformedRequest,
            "operator request I/O failed",
        ),
    };
    OperatorRemoteErrorV2 {
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
        let config =
            ServerOperatorConfig::new(directory.join("operator.sock"), Duration::from_millis(100))
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
    fn frame_header_is_exact_big_endian_nbop_v2() {
        let request = OperatorRequestV2 {
            request_id: 42,
            operation: OperatorOperationV2::Status {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..6], &[0, 2]);
        assert_eq!(&bytes[6..8], &[0, 0]);
        assert_eq!(
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize,
            bytes.len() - OPERATOR_HEADER_BYTES
        );
        let decoded: OperatorRequestV2 = read_frame(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert!(matches!(decoded.operation, OperatorOperationV2::Status {}));
    }

    #[test]
    fn response_frame_and_request_id_echo_are_stable() {
        let response = OperatorResponseV2::Ok {
            request_id: 42,
            result: OperatorResultV2::SchedulerReset {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &response).unwrap();
        let payload = br#"{"outcome":"ok","request_id":42,"result":{"type":"scheduler_reset"}}"#;
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..8], &[0, 2, 0, 0]);
        assert_eq!(&bytes[8..12], &(payload.len() as u32).to_be_bytes());
        assert_eq!(&bytes[12..], payload);
    }

    #[test]
    fn physical_design_status_and_report_use_explicit_stable_dtos() {
        let status = operator_physical_design_status(ServerPhysicalDesignStatus {
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
                last_record_error: Some(PhysicalDesignEvidenceRecordError::OutOfOrderVisibility {
                    previous: netbadb_types::DatabaseCommitSeq(9),
                    received: netbadb_types::DatabaseCommitSeq(8),
                }),
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
        });
        assert_eq!(status.diagnostics.eligible_query_count, 1);
        assert_eq!(
            status.diagnostics.last_record_outcome,
            Some(OperatorPhysicalDesignRecordOutcomeV2::SchemaRotatedWithCapacityRejection)
        );
        assert_eq!(
            status.diagnostics.last_record_error,
            Some(OperatorPhysicalDesignRecordErrorV2::OutOfOrderVisibility)
        );
        assert_eq!(
            status.evidence.limits.max_columnar_columns_per_candidate,
            13
        );
        assert_eq!(status.evidence.epoch, 14);
        assert_eq!(status.evidence.ordering_high_water, Some(18));
        assert_eq!(status.evidence.discarded_incomplete_reports, 23);

        for (reason, expected) in [
            (
                PhysicalDesignNoActionReason::BelowMinimumReports,
                OperatorPhysicalDesignNoActionReasonV2::BelowMinimumReports,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumShapeDiversity,
                OperatorPhysicalDesignNoActionReasonV2::BelowMinimumShapeDiversity,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumActualWork,
                OperatorPhysicalDesignNoActionReasonV2::BelowMinimumActualWork,
            ),
            (
                PhysicalDesignNoActionReason::ExistingDesignCovers,
                OperatorPhysicalDesignNoActionReasonV2::ExistingDesignCovers,
            ),
            (
                PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
                OperatorPhysicalDesignNoActionReasonV2::UnsupportedCurrentLayout,
            ),
            (
                PhysicalDesignNoActionReason::IncompleteEvidence,
                OperatorPhysicalDesignNoActionReasonV2::IncompleteEvidence,
            ),
            (
                PhysicalDesignNoActionReason::CurrentProjectionUnavailable,
                OperatorPhysicalDesignNoActionReasonV2::CurrentProjectionUnavailable,
            ),
            (
                PhysicalDesignNoActionReason::RecommendationLimitReached,
                OperatorPhysicalDesignNoActionReasonV2::RecommendationLimitReached,
            ),
        ] {
            assert_eq!(operator_no_action_reason(reason), expected);
        }
    }

    #[test]
    fn oversized_success_becomes_bounded_response_too_large_error() {
        let candidate = OperatorPhysicalIndexCandidateV2 {
            table_id: 1,
            column_id: 2,
            point_report_count: 3,
            range_report_count: 4,
            evidence: OperatorPhysicalDesignEvidenceSummaryV2 {
                report_count: 5,
                distinct_query_shapes: 6,
                total_actual_scan_work_units: 7,
                total_rows_examined: 8,
                overflowed: false,
                incomplete: false,
                truncated: false,
            },
            decision: OperatorPhysicalDesignDecisionV2::Recommend {},
        };
        let response = OperatorResponseV2::Ok {
            request_id: 77,
            result: OperatorResultV2::PhysicalDesignRecommendations {
                report: OperatorPhysicalDesignAdvisorReportV2 {
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
            read_frame::<OperatorResponseV2>(&mut bytes.as_slice()).unwrap(),
            OperatorResponseV2::Error {
                request_id: 77,
                error: OperatorRemoteErrorV2 {
                    code: OperatorErrorCodeV2::ResponseTooLarge,
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
            OperatorRequestV2 {
                request_id: 11,
                operation: OperatorOperationV2::ResetFaultedScheduler {},
            },
            &control,
            &design_control,
        );
        assert!(matches!(
            reset,
            OperatorResponseV2::Ok {
                request_id: 11,
                result: OperatorResultV2::SchedulerReset {}
            }
        ));

        let stale = execute_operator_request(
            OperatorRequestV2 {
                request_id: 12,
                operation: OperatorOperationV2::RotateEvidence {
                    expected_window_epoch: 7,
                },
            },
            &control,
            &design_control,
        );
        assert!(matches!(
            stale,
            OperatorResponseV2::Error {
                request_id: 12,
                error: OperatorRemoteErrorV2 {
                    code: OperatorErrorCodeV2::EvidenceWindowChanged,
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
        wrong_magic[4..6].copy_from_slice(&2_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut wrong_magic.as_slice()),
            Err(OperatorProtocolError::WrongMagic)
        ));

        let mut wrong_version = [0_u8; OPERATOR_HEADER_BYTES];
        wrong_version[..4].copy_from_slice(b"NBOP");
        wrong_version[4..6].copy_from_slice(&1_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut wrong_version.as_slice()),
            Err(OperatorProtocolError::UnsupportedVersion(1))
        ));

        let mut reserved = [0_u8; OPERATOR_HEADER_BYTES];
        reserved[..4].copy_from_slice(b"NBOP");
        reserved[4..6].copy_from_slice(&2_u16.to_be_bytes());
        reserved[6..8].copy_from_slice(&1_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut reserved.as_slice()),
            Err(OperatorProtocolError::NonzeroReserved(1))
        ));

        let mut oversized = [0_u8; OPERATOR_HEADER_BYTES];
        oversized[..4].copy_from_slice(b"NBOP");
        oversized[4..6].copy_from_slice(&2_u16.to_be_bytes());
        oversized[8..12].copy_from_slice(&(MAX_OPERATOR_PAYLOAD_BYTES + 1).to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut oversized.as_slice()),
            Err(OperatorProtocolError::RequestTooLarge(_))
        ));
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut b"NB".as_slice()),
            Err(OperatorProtocolError::TruncatedHeader)
        ));

        let truncated_payload = b"NBOP\0\x02\0\0\0\0\0\x02{".to_vec();
        assert!(matches!(
            read_frame::<OperatorRequestV2>(&mut truncated_payload.as_slice()),
            Err(OperatorProtocolError::TruncatedPayload)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn v1_client_receives_explicit_unsupported_protocol_version() {
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
                ),
                Err(OperatorProtocolError::UnsupportedVersion(1))
            ));
        });
        let mut v1_header = [0_u8; OPERATOR_HEADER_BYTES];
        v1_header[..4].copy_from_slice(b"NBOP");
        v1_header[4..6].copy_from_slice(&1_u16.to_be_bytes());
        client.write_all(&v1_header).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV2>(&mut client).unwrap(),
            OperatorResponseV2::Error {
                request_id: 0,
                error: OperatorRemoteErrorV2 {
                    code: OperatorErrorCodeV2::UnsupportedProtocolVersion,
                    ..
                }
            }
        ));
        worker.join().unwrap();
    }

    #[test]
    fn strict_request_json_rejects_unknown_fields_and_operations() {
        fn framed(payload: &[u8]) -> Vec<u8> {
            let mut bytes = Vec::from(b"NBOP\0\x02\0\0".as_slice());
            bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            bytes.extend_from_slice(payload);
            bytes
        }
        for payload in [
            br#"{"request_id":1,"operation":{"type":"status","extra":true}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"unknown"}}"#.as_slice(),
            &[0xff][..],
        ] {
            assert!(matches!(
                read_frame::<OperatorRequestV2>(&mut framed(payload).as_slice()),
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
                &OperatorRequestV2 {
                    request_id,
                    operation: OperatorOperationV2::ResetFaultedScheduler {},
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
            read_frame::<OperatorResponseV2>(&mut first).unwrap(),
            OperatorResponseV2::Ok { request_id: 1, .. }
        ));

        let second_reply = match control_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => reply,
            _ => panic!("unexpected second operator control request"),
        };
        second_reply
            .send(Err(ServerAdaptiveControlError::SchedulerNotFaulted))
            .unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV2>(&mut second).unwrap(),
            OperatorResponseV2::Error {
                request_id: 2,
                error: OperatorRemoteErrorV2 {
                    code: OperatorErrorCodeV2::SchedulerNotFaulted,
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
