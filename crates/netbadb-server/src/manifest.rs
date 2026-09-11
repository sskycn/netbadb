use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use netbadb_core::{
    AdaptiveChangeStreamGcPolicy, AdaptiveColumnarCompactionPolicy, AdaptiveEvidencePoolLimits,
    AdaptiveLsmCompactionPolicy, AdaptiveLsmFlushPolicy, AdaptivePolicy, AdaptiveWorkloadLimits,
    AdaptiveWorkloadPolicy, AutomaticCalibrationTrialPolicy, AutomaticCrossLaneServicePolicy,
    AutomaticMultiSafeModePolicy, AutomaticOrchestrationEnvelope, AutomaticSafeModePolicy,
    AutomaticSchedulerPolicy, AutomaticSchedulerPolicyError, CalibrationRatio,
    CalibrationRatioError, MaintenanceBudget, PhysicalDesignAdvisorPolicy,
    PhysicalDesignEvidenceLimits, PhysicalDesignRecommendationPolicy, PlannerCalibrationClass,
    PlannerCalibrationPolicy,
};
use netbadb_schema::{ColumnDef, Schema, SchemaError, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde::{Deserialize, Deserializer};

use crate::ServerPhysicalDesignAdvisorConfig;
use crate::adaptive_driver::ServerAdaptiveStartupMode;
use crate::authorization::{AuthorizationPolicy, TablePermissions, parse_certificate_sha256};
use crate::tls::{MutualTlsConfig, TlsMaterialPaths, TransportSecurity};
use crate::{
    AuthorizationConfigError, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_RESULT_ROWS, DEFAULT_WRITE_TIMEOUT, ServerLimits, ServerLimitsError,
};
use crate::{
    ServerAdaptiveDriverConfig, ServerAdaptiveDriverConfigError, ServerAdaptiveFeedbackConfig,
    ServerAdaptiveMode,
};
use crate::{ServerOperatorConfig, ServerOperatorConfigError};
use crate::{TlsConfigError, TransportKind};

pub const DEPLOYMENT_MANIFEST_VERSION: u32 = 7;
pub const DEFAULT_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7878);

#[derive(Debug, Clone, PartialEq, Eq)]
/// Required exact table expectation and physical locator. Provisioning creates
/// the catalog once; ordinary server startup never installs or repairs it.
pub struct TableBootstrap {
    pub path: PathBuf,
    pub table: TableDef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    listen: SocketAddr,
    tables: Vec<TableBootstrap>,
    limits: ServerLimits,
    tls: Option<MutualTlsConfig>,
    authorization: AuthorizationPolicy,
    adaptive_mode: ServerAdaptiveStartupMode,
    physical_design: Option<ServerPhysicalDesignAdvisorConfig>,
    operator: Option<ServerOperatorConfig>,
}

pub(crate) type ServerConfigParts = (
    SocketAddr,
    Vec<TableBootstrap>,
    ServerLimits,
    TransportSecurity,
    AuthorizationPolicy,
    ServerAdaptiveStartupMode,
    Option<ServerPhysicalDesignAdvisorConfig>,
    Option<ServerOperatorConfig>,
);

impl ServerConfig {
    pub fn from_manifest_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let version: ManifestVersion =
            serde_json::from_str(&source).map_err(ManifestError::Json)?;
        if version.version != DEPLOYMENT_MANIFEST_VERSION {
            return Err(ManifestError::UnsupportedVersion(version.version));
        }
        let manifest: DeploymentManifest =
            serde_json::from_str(&source).map_err(ManifestError::Json)?;
        if manifest.tables.is_empty() {
            return Err(ManifestError::EmptyTables);
        }
        let operator = manifest.operator;
        let adaptive_mode = manifest
            .adaptive
            .map_or(Ok(ServerAdaptiveStartupMode::Disabled), |adaptive| {
                adaptive.into_runtime()
            })?;
        let physical_design = manifest
            .physical_design
            .map(ManifestPhysicalDesign::into_config);
        if operator.is_some()
            && matches!(adaptive_mode, ServerAdaptiveStartupMode::Disabled)
            && physical_design.is_none()
        {
            return Err(ManifestError::OperatorRequiresManagedRuntime);
        }

        let listen = match manifest.listen {
            Some(value) => value
                .parse()
                .map_err(|source| ManifestError::InvalidListen { value, source })?,
            None => DEFAULT_LISTEN_ADDRESS,
        };
        let limits = manifest
            .limits
            .map_or_else(|| Ok(ServerLimits::default()), ManifestLimits::into_limits)
            .map_err(ManifestError::Limits)?;

        let manifest_directory = path
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|source| ManifestError::ManifestDirectory {
                path: path.to_path_buf(),
                source,
            })?;
        let operator = operator
            .map(|operator| operator.into_config(&manifest_directory))
            .transpose()?;
        let tls = manifest
            .tls
            .map(|tls| {
                let paths = tls.resolve(&manifest_directory)?;
                MutualTlsConfig::load(paths).map_err(ManifestError::TlsConfiguration)
            })
            .transpose()?;
        validate_listener_security(listen, tls.is_some())?;
        let mut tables = Vec::with_capacity(manifest.tables.len());
        let mut paths = HashSet::with_capacity(manifest.tables.len());
        for table in manifest.tables {
            let configured_path = PathBuf::from(&table.path);
            let resolved_path = if configured_path.is_absolute() {
                configured_path
            } else {
                manifest_directory.join(configured_path)
            };
            let resolved_path =
                resolved_path
                    .canonicalize()
                    .map_err(|source| ManifestError::TablePath {
                        path: resolved_path,
                        source,
                    })?;
            let metadata =
                std::fs::metadata(&resolved_path).map_err(|source| ManifestError::TablePath {
                    path: resolved_path.clone(),
                    source,
                })?;
            if !metadata.is_file() {
                return Err(ManifestError::TablePathIsNotFile(resolved_path));
            }
            if !paths.insert(resolved_path.clone()) {
                return Err(ManifestError::DuplicateStoragePath(resolved_path));
            }

            let columns = table
                .columns
                .into_iter()
                .map(ManifestColumn::into_column)
                .collect();
            let table = TableDef::new(TableId(table.id), table.name, columns);
            table.validate().map_err(ManifestError::Schema)?;
            table.fingerprint().map_err(ManifestError::Schema)?;
            tables.push(TableBootstrap {
                path: resolved_path,
                table,
            });
        }
        Schema::new(tables.iter().map(|entry| entry.table.clone()).collect())
            .map_err(ManifestError::Schema)?;
        let transport = if tls.is_some() {
            TransportKind::MutualTls
        } else {
            TransportKind::PlaintextLoopback
        };
        let known_tables = tables
            .iter()
            .map(|entry| entry.table.id)
            .collect::<Vec<_>>();
        let authorization = manifest
            .authorization
            .into_policy(transport, &known_tables)
            .map_err(ManifestError::Authorization)?;
        Ok(Self {
            listen,
            tables,
            limits,
            tls,
            authorization,
            adaptive_mode,
            physical_design,
            operator,
        })
    }

    #[must_use]
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub fn tables(&self) -> &[TableBootstrap] {
        &self.tables
    }

    #[must_use]
    pub const fn limits(&self) -> ServerLimits {
        self.limits
    }

    #[must_use]
    pub const fn transport_kind(&self) -> TransportKind {
        if self.tls.is_some() {
            TransportKind::MutualTls
        } else {
            TransportKind::PlaintextLoopback
        }
    }

    /// Returns the deployment-selected adaptive runtime mode without exposing
    /// private manifest representation or runtime state.
    #[must_use]
    pub const fn adaptive_mode(&self) -> ServerAdaptiveMode {
        self.adaptive_mode.mode()
    }

    /// Returns whether manifest-derived physical-design observation is enabled.
    #[must_use]
    pub const fn physical_design_enabled(&self) -> bool {
        self.physical_design.is_some()
    }

    /// Returns the complete manifest-derived physical-design configuration.
    #[must_use]
    pub const fn physical_design_config(&self) -> Option<&ServerPhysicalDesignAdvisorConfig> {
        self.physical_design.as_ref()
    }

    /// Returns the resolved local operator configuration without binding or
    /// connecting to its socket.
    #[must_use]
    pub const fn operator_config(&self) -> Option<&ServerOperatorConfig> {
        self.operator.as_ref()
    }

    pub(crate) fn into_parts(self) -> ServerConfigParts {
        let security = self
            .tls
            .map_or(TransportSecurity::PlaintextLoopback, |tls| {
                tls.into_transport()
            });
        (
            self.listen,
            self.tables,
            self.limits,
            security,
            self.authorization,
            self.adaptive_mode,
            self.physical_design,
            self.operator,
        )
    }
}

pub(crate) fn validate_listener_security(
    address: SocketAddr,
    mutual_tls: bool,
) -> Result<(), ManifestError> {
    if address.ip().is_loopback() || mutual_tls {
        Ok(())
    } else {
        Err(ManifestError::RemoteListenRequiresMutualTls(address))
    }
}

#[derive(Debug)]
pub enum ManifestError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    EmptyTables,
    InvalidListen {
        value: String,
        source: AddrParseError,
    },
    RemoteListenRequiresMutualTls(SocketAddr),
    ManifestDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    TablePath {
        path: PathBuf,
        source: std::io::Error,
    },
    TablePathIsNotFile(PathBuf),
    DuplicateStoragePath(PathBuf),
    Limits(ServerLimitsError),
    TlsPath {
        field: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    TlsPathIsNotFile {
        field: &'static str,
        path: PathBuf,
    },
    TlsConfiguration(TlsConfigError),
    Authorization(AuthorizationConfigError),
    AdaptiveSchedulerPolicy(AutomaticSchedulerPolicyError),
    CalibrationRatio {
        field: &'static str,
        source: CalibrationRatioError,
    },
    AdaptiveDriverConfig(ServerAdaptiveDriverConfigError),
    OperatorRequiresManagedRuntime,
    OperatorSocketPath(PathBuf),
    OperatorSocketParent {
        path: PathBuf,
        source: std::io::Error,
    },
    OperatorSocketParentNotDirectory(PathBuf),
    OperatorConfig(ServerOperatorConfigError),
    Schema(SchemaError),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read manifest `{}`: {source}",
                    path.display()
                )
            }
            Self::Json(error) => write!(formatter, "invalid deployment manifest JSON: {error}"),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported deployment manifest version {version}; expected {DEPLOYMENT_MANIFEST_VERSION}"
            ),
            Self::EmptyTables => {
                formatter.write_str("deployment manifest requires at least one table")
            }
            Self::InvalidListen { value, .. } => {
                write!(formatter, "invalid TCP listen address `{value}`")
            }
            Self::RemoteListenRequiresMutualTls(address) => write!(
                formatter,
                "non-loopback listener `{address}` requires mutual TLS"
            ),
            Self::ManifestDirectory { path, source } => write!(
                formatter,
                "failed to resolve directory containing manifest `{}`: {source}",
                path.display()
            ),
            Self::TablePath { path, source } => write!(
                formatter,
                "failed to resolve table file `{}`: {source}",
                path.display()
            ),
            Self::TablePathIsNotFile(path) => {
                write!(formatter, "table path `{}` is not a file", path.display())
            }
            Self::DuplicateStoragePath(path) => write!(
                formatter,
                "table file `{}` is configured more than once",
                path.display()
            ),
            Self::Limits(error) => error.fmt(formatter),
            Self::TlsPath {
                field,
                path,
                source,
            } => write!(
                formatter,
                "failed to resolve TLS field `{field}` path `{}`: {source}",
                path.display()
            ),
            Self::TlsPathIsNotFile { field, path } => write!(
                formatter,
                "TLS field `{field}` path `{}` is not a file",
                path.display()
            ),
            Self::TlsConfiguration(error) => error.fmt(formatter),
            Self::Authorization(error) => error.fmt(formatter),
            Self::AdaptiveSchedulerPolicy(error) => {
                write!(formatter, "invalid adaptive scheduler policy: {error}")
            }
            Self::CalibrationRatio { field, source } => {
                write!(
                    formatter,
                    "invalid adaptive calibration ratio `{field}`: {source}"
                )
            }
            Self::AdaptiveDriverConfig(error) => {
                write!(formatter, "invalid adaptive driver configuration: {error}")
            }
            Self::OperatorRequiresManagedRuntime => formatter
                .write_str("operator plane requires Adaptive or Physical Design to be enabled"),
            Self::OperatorSocketPath(path) => write!(
                formatter,
                "operator Unix socket path `{}` must name a file",
                path.display()
            ),
            Self::OperatorSocketParent { path, source } => write!(
                formatter,
                "failed to resolve parent directory for operator socket `{}`: {source}",
                path.display()
            ),
            Self::OperatorSocketParentNotDirectory(path) => write!(
                formatter,
                "operator socket parent `{}` is not a directory",
                path.display()
            ),
            Self::OperatorConfig(error) => {
                write!(formatter, "invalid operator configuration: {error}")
            }
            Self::Schema(error) => error.fmt(formatter),
        }
    }
}

impl Error for ManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. }
            | Self::ManifestDirectory { source, .. }
            | Self::TablePath { source, .. }
            | Self::TlsPath { source, .. }
            | Self::OperatorSocketParent { source, .. } => Some(source),
            Self::Json(error) => Some(error),
            Self::InvalidListen { source, .. } => Some(source),
            Self::Limits(error) => Some(error),
            Self::TlsConfiguration(error) => Some(error),
            Self::Authorization(error) => Some(error),
            Self::AdaptiveSchedulerPolicy(error) => Some(error),
            Self::CalibrationRatio { source, .. } => Some(source),
            Self::AdaptiveDriverConfig(error) => Some(error),
            Self::OperatorConfig(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::UnsupportedVersion(_)
            | Self::EmptyTables
            | Self::RemoteListenRequiresMutualTls(_)
            | Self::TablePathIsNotFile(_)
            | Self::DuplicateStoragePath(_)
            | Self::TlsPathIsNotFile { .. }
            | Self::OperatorRequiresManagedRuntime
            | Self::OperatorSocketPath(_)
            | Self::OperatorSocketParentNotDirectory(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ManifestVersion {
    version: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentManifest {
    #[serde(rename = "version")]
    _version: u32,
    listen: Option<String>,
    limits: Option<ManifestLimits>,
    tls: Option<ManifestTls>,
    authorization: ManifestAuthorization,
    tables: Vec<ManifestTable>,
    #[serde(default, deserialize_with = "deserialize_optional_adaptive")]
    adaptive: Option<ManifestAdaptive>,
    #[serde(default, deserialize_with = "deserialize_optional_physical_design")]
    physical_design: Option<ManifestPhysicalDesign>,
    #[serde(default, deserialize_with = "deserialize_optional_operator")]
    operator: Option<ManifestOperator>,
}

fn deserialize_optional_physical_design<'de, D>(
    deserializer: D,
) -> Result<Option<ManifestPhysicalDesign>, D::Error>
where
    D: Deserializer<'de>,
{
    ManifestPhysicalDesign::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPhysicalDesign {
    evidence_limits: ManifestPhysicalDesignEvidenceLimits,
    advisor_policy: ManifestPhysicalDesignAdvisorPolicy,
}

impl ManifestPhysicalDesign {
    const fn into_config(self) -> ServerPhysicalDesignAdvisorConfig {
        ServerPhysicalDesignAdvisorConfig::new(
            self.evidence_limits.into_limits(),
            self.advisor_policy.into_policy(),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPhysicalDesignEvidenceLimits {
    max_index_candidates: u64,
    max_columnar_candidates: u64,
    max_query_shapes_per_candidate: u64,
    max_columnar_columns_per_candidate: u64,
}

impl ManifestPhysicalDesignEvidenceLimits {
    const fn into_limits(self) -> PhysicalDesignEvidenceLimits {
        PhysicalDesignEvidenceLimits {
            max_index_candidates: self.max_index_candidates,
            max_columnar_candidates: self.max_columnar_candidates,
            max_query_shapes_per_candidate: self.max_query_shapes_per_candidate,
            max_columnar_columns_per_candidate: self.max_columnar_columns_per_candidate,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPhysicalDesignAdvisorPolicy {
    index: ManifestPhysicalDesignRecommendationPolicy,
    columnar: ManifestPhysicalDesignRecommendationPolicy,
}

impl ManifestPhysicalDesignAdvisorPolicy {
    const fn into_policy(self) -> PhysicalDesignAdvisorPolicy {
        PhysicalDesignAdvisorPolicy {
            index: self.index.into_policy(),
            columnar: self.columnar.into_policy(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPhysicalDesignRecommendationPolicy {
    minimum_reports: u64,
    minimum_distinct_query_shapes: u64,
    minimum_actual_scan_work_units: u64,
    max_recommendations: u32,
}

impl ManifestPhysicalDesignRecommendationPolicy {
    const fn into_policy(self) -> PhysicalDesignRecommendationPolicy {
        PhysicalDesignRecommendationPolicy {
            minimum_reports: self.minimum_reports,
            minimum_distinct_query_shapes: self.minimum_distinct_query_shapes,
            minimum_actual_scan_work_units: self.minimum_actual_scan_work_units,
            max_recommendations: self.max_recommendations,
        }
    }
}

fn deserialize_optional_operator<'de, D>(
    deserializer: D,
) -> Result<Option<ManifestOperator>, D::Error>
where
    D: Deserializer<'de>,
{
    ManifestOperator::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestOperator {
    unix_socket: String,
    io_timeout_ms: u64,
}

impl ManifestOperator {
    fn into_config(self, manifest_directory: &Path) -> Result<ServerOperatorConfig, ManifestError> {
        let configured = PathBuf::from(self.unix_socket);
        let joined = if configured.is_absolute() {
            configured
        } else {
            manifest_directory.join(configured)
        };
        let file_name = joined
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| ManifestError::OperatorSocketPath(joined.clone()))?;
        let parent = joined
            .parent()
            .ok_or_else(|| ManifestError::OperatorSocketPath(joined.clone()))?;
        let parent =
            parent
                .canonicalize()
                .map_err(|source| ManifestError::OperatorSocketParent {
                    path: joined.clone(),
                    source,
                })?;
        if !parent.is_dir() {
            return Err(ManifestError::OperatorSocketParentNotDirectory(parent));
        }
        ServerOperatorConfig::new(
            parent.join(file_name),
            Duration::from_millis(self.io_timeout_ms),
        )
        .map_err(ManifestError::OperatorConfig)
    }
}

fn deserialize_optional_adaptive<'de, D>(
    deserializer: D,
) -> Result<Option<ManifestAdaptive>, D::Error>
where
    D: Deserializer<'de>,
{
    ManifestAdaptive::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestAdaptive {
    FeedbackOnly {
        feedback: ManifestAdaptiveFeedback,
    },
    Driven {
        feedback: ManifestAdaptiveFeedback,
        host: ManifestAdaptiveHost,
        scheduler_policy: ManifestSchedulerPolicy,
        orchestration_envelope: ManifestOrchestrationEnvelope,
        scope: ManifestAdaptiveScope,
        automatic_policy: Box<ManifestAutomaticPolicy>,
    },
}

impl ManifestAdaptive {
    fn into_runtime(self) -> Result<ServerAdaptiveStartupMode, ManifestError> {
        match self {
            Self::FeedbackOnly { feedback } => Ok(ServerAdaptiveStartupMode::FeedbackOnly(
                feedback.into_config(),
            )),
            Self::Driven {
                feedback,
                host,
                scheduler_policy,
                orchestration_envelope,
                scope,
                automatic_policy,
            } => {
                let scheduler_policy = scheduler_policy
                    .into_policy()
                    .map_err(ManifestError::AdaptiveSchedulerPolicy)?;
                let automatic_policy = (*automatic_policy).into_policy()?;
                let config = ServerAdaptiveDriverConfig::new(
                    feedback.into_config(),
                    Duration::from_millis(host.tick_interval_ms),
                    scheduler_policy,
                    orchestration_envelope.into_envelope(),
                    automatic_policy,
                    scope.table_ids.into_iter().map(TableId).collect(),
                    scope
                        .calibration_classes
                        .into_iter()
                        .map(ManifestCalibrationClass::into_class)
                        .collect(),
                )
                .map_err(ManifestError::AdaptiveDriverConfig)?;
                Ok(ServerAdaptiveStartupMode::Driven(Box::new(config)))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAdaptiveFeedback {
    limits: ManifestEvidencePoolLimits,
}

impl ManifestAdaptiveFeedback {
    const fn into_config(self) -> ServerAdaptiveFeedbackConfig {
        ServerAdaptiveFeedbackConfig::new(AdaptiveEvidencePoolLimits {
            max_target_windows: self.limits.max_target_windows,
            workload_limits: AdaptiveWorkloadLimits::new(
                self.limits.workload.max_query_shapes,
                self.limits.workload.max_plan_variants_per_shape,
            ),
            max_calibration_epochs: self.limits.max_calibration_epochs,
            max_calibration_query_shapes: self.limits.max_calibration_query_shapes,
            max_calibration_plan_variants_per_shape: self
                .limits
                .max_calibration_plan_variants_per_shape,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestEvidencePoolLimits {
    max_target_windows: u64,
    workload: ManifestWorkloadLimits,
    max_calibration_epochs: u64,
    max_calibration_query_shapes: u64,
    max_calibration_plan_variants_per_shape: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWorkloadLimits {
    max_query_shapes: u64,
    max_plan_variants_per_shape: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAdaptiveHost {
    tick_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSchedulerPolicy {
    minimum_ticks_between_runs: u64,
    idle_retry_ticks: u64,
    no_progress_retry_ticks: u64,
    trial_retry_ticks: u64,
}

impl ManifestSchedulerPolicy {
    fn into_policy(self) -> Result<AutomaticSchedulerPolicy, AutomaticSchedulerPolicyError> {
        AutomaticSchedulerPolicy::new(
            self.minimum_ticks_between_runs,
            self.idle_retry_ticks,
            self.no_progress_retry_ticks,
            self.trial_retry_ticks,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestOrchestrationEnvelope {
    max_steps: u32,
    per_step_maintenance_budget: ManifestMaintenanceBudget,
    run_maintenance_budget: ManifestMaintenanceBudget,
}

impl ManifestOrchestrationEnvelope {
    const fn into_envelope(self) -> AutomaticOrchestrationEnvelope {
        AutomaticOrchestrationEnvelope {
            max_steps: self.max_steps,
            per_step_maintenance_budget: self.per_step_maintenance_budget.into_budget(),
            run_maintenance_budget: self.run_maintenance_budget.into_budget(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestMaintenanceBudget {
    max_work_units: u64,
    max_read_bytes: u64,
    max_write_bytes: u64,
    max_actions: u32,
}

impl ManifestMaintenanceBudget {
    const fn into_budget(self) -> MaintenanceBudget {
        MaintenanceBudget::new(
            self.max_work_units,
            self.max_read_bytes,
            self.max_write_bytes,
            self.max_actions,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAdaptiveScope {
    table_ids: Vec<u64>,
    calibration_classes: Vec<ManifestCalibrationClass>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestCalibrationClass {
    SeqScan,
    IndexPoint,
    IndexRange,
    Columnar,
}

impl ManifestCalibrationClass {
    const fn into_class(self) -> PlannerCalibrationClass {
        match self {
            Self::SeqScan => PlannerCalibrationClass::SeqScan,
            Self::IndexPoint => PlannerCalibrationClass::IndexPoint,
            Self::IndexRange => PlannerCalibrationClass::IndexRange,
            Self::Columnar => PlannerCalibrationClass::Columnar,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAutomaticPolicy {
    safe_mode: ManifestSafeModePolicy,
    allow_columnar_compaction: bool,
    allow_change_stream_gc: bool,
    allow_lsm_flush: bool,
    allow_lsm_compaction: bool,
    change_stream_gc_policy: ManifestChangeStreamGcPolicy,
    lsm_flush_policy: ManifestLsmFlushPolicy,
    lsm_compaction_policy: ManifestLsmCompactionPolicy,
    columnar_compaction_policy: ManifestColumnarCompactionPolicy,
    cross_lane_service: ManifestCrossLaneService,
    max_candidate_tables: u64,
    max_calibration_classes: u64,
    max_fairness_entries: u64,
}

impl ManifestAutomaticPolicy {
    fn into_policy(self) -> Result<AutomaticMultiSafeModePolicy, ManifestError> {
        Ok(AutomaticMultiSafeModePolicy {
            safe_mode: self.safe_mode.into_policy()?,
            allow_columnar_compaction: self.allow_columnar_compaction,
            allow_change_stream_gc: self.allow_change_stream_gc,
            allow_lsm_flush: self.allow_lsm_flush,
            allow_lsm_compaction: self.allow_lsm_compaction,
            change_stream_gc_policy: AdaptiveChangeStreamGcPolicy::new(
                self.change_stream_gc_policy.minimum_reclaimable_batches,
                self.change_stream_gc_policy.minimum_reclaimable_bytes,
            ),
            lsm_flush_policy: AdaptiveLsmFlushPolicy {
                minimum_memtable_bytes: self.lsm_flush_policy.minimum_memtable_bytes,
            },
            lsm_compaction_policy: AdaptiveLsmCompactionPolicy {
                minimum_input_bytes: self.lsm_compaction_policy.minimum_input_bytes,
            },
            columnar_compaction_policy: AdaptiveColumnarCompactionPolicy::new(
                self.columnar_compaction_policy.minimum_delta_segments,
                self.columnar_compaction_policy.minimum_delta_bytes,
            ),
            cross_lane_service: self.cross_lane_service.into_policy(),
            max_candidate_tables: self.max_candidate_tables,
            max_calibration_classes: self.max_calibration_classes,
            max_fairness_entries: self.max_fairness_entries,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSafeModePolicy {
    allow_columnar_maintenance: bool,
    allow_planner_calibration: bool,
    adaptive_policy: ManifestAdaptivePolicy,
    workload_policy: ManifestWorkloadPolicy,
    planner_calibration_policy: ManifestPlannerCalibrationPolicy,
    calibration_trial_policy: ManifestCalibrationTrialPolicy,
}

impl ManifestSafeModePolicy {
    fn into_policy(self) -> Result<AutomaticSafeModePolicy, ManifestError> {
        Ok(AutomaticSafeModePolicy {
            allow_columnar_maintenance: self.allow_columnar_maintenance,
            allow_planner_calibration: self.allow_planner_calibration,
            adaptive_policy: AdaptivePolicy::new(
                self.adaptive_policy.minimum_expected_benefit_work_units,
                self.adaptive_policy.minimum_keep_benefit_work_units,
            ),
            workload_policy: AdaptiveWorkloadPolicy::new(
                self.workload_policy.minimum_samples,
                self.workload_policy.minimum_actual_work_units,
                self.workload_policy.minimum_distinct_visibility_points,
                self.workload_policy.minimum_keep_improvement_work_units,
                self.workload_policy.maximum_tolerated_regression_work_units,
            ),
            planner_calibration_policy: self.planner_calibration_policy.into_policy()?,
            calibration_trial_policy: AutomaticCalibrationTrialPolicy {
                minimum_samples: self.calibration_trial_policy.minimum_samples,
                minimum_actual_work_units: self.calibration_trial_policy.minimum_actual_work_units,
                minimum_distinct_visibility_points: self
                    .calibration_trial_policy
                    .minimum_distinct_visibility_points,
                minimum_distinct_query_shapes: self
                    .calibration_trial_policy
                    .minimum_distinct_query_shapes,
                minimum_keep_error_improvement_work_units: self
                    .calibration_trial_policy
                    .minimum_keep_error_improvement_work_units,
                maximum_tolerated_error_regression_work_units: self
                    .calibration_trial_policy
                    .maximum_tolerated_error_regression_work_units,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAdaptivePolicy {
    minimum_expected_benefit_work_units: u64,
    minimum_keep_benefit_work_units: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWorkloadPolicy {
    minimum_samples: u64,
    minimum_actual_work_units: u64,
    minimum_distinct_visibility_points: u64,
    minimum_keep_improvement_work_units: u64,
    maximum_tolerated_regression_work_units: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPlannerCalibrationPolicy {
    minimum_samples: u64,
    minimum_actual_work_units: u64,
    minimum_distinct_visibility_points: u64,
    minimum_distinct_query_shapes: u64,
    minimum_directional_query_shape_margin: u64,
    error_deadband_work_units: u64,
    minimum_shadow_error_improvement_work_units: u64,
    global_min_ratio: ManifestCalibrationRatio,
    global_max_ratio: ManifestCalibrationRatio,
    maximum_step_up_ratio: ManifestCalibrationRatio,
    maximum_step_down_ratio: ManifestCalibrationRatio,
}

impl ManifestPlannerCalibrationPolicy {
    fn into_policy(self) -> Result<PlannerCalibrationPolicy, ManifestError> {
        Ok(PlannerCalibrationPolicy {
            minimum_samples: self.minimum_samples,
            minimum_actual_work_units: self.minimum_actual_work_units,
            minimum_distinct_visibility_points: self.minimum_distinct_visibility_points,
            minimum_distinct_query_shapes: self.minimum_distinct_query_shapes,
            minimum_directional_query_shape_margin: self.minimum_directional_query_shape_margin,
            error_deadband_work_units: self.error_deadband_work_units,
            minimum_shadow_error_improvement_work_units: self
                .minimum_shadow_error_improvement_work_units,
            global_min_ratio: self.global_min_ratio.into_ratio("global_min_ratio")?,
            global_max_ratio: self.global_max_ratio.into_ratio("global_max_ratio")?,
            maximum_step_up_ratio: self
                .maximum_step_up_ratio
                .into_ratio("maximum_step_up_ratio")?,
            maximum_step_down_ratio: self
                .maximum_step_down_ratio
                .into_ratio("maximum_step_down_ratio")?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestCalibrationRatio {
    numerator: u64,
    denominator: u64,
}

impl ManifestCalibrationRatio {
    fn into_ratio(self, field: &'static str) -> Result<CalibrationRatio, ManifestError> {
        CalibrationRatio::new(self.numerator, self.denominator)
            .map_err(|source| ManifestError::CalibrationRatio { field, source })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestCalibrationTrialPolicy {
    minimum_samples: u64,
    minimum_actual_work_units: u64,
    minimum_distinct_visibility_points: u64,
    minimum_distinct_query_shapes: u64,
    minimum_keep_error_improvement_work_units: u64,
    maximum_tolerated_error_regression_work_units: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestColumnarCompactionPolicy {
    minimum_delta_segments: u64,
    minimum_delta_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestChangeStreamGcPolicy {
    minimum_reclaimable_batches: u64,
    minimum_reclaimable_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLsmFlushPolicy {
    minimum_memtable_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLsmCompactionPolicy {
    minimum_input_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestCrossLaneService {
    StrictPhysicalPriority(ManifestStrictCrossLaneService),
    BoundedColumnarBurst(ManifestBoundedColumnarBurst),
    BoundedFourLaneCycle(ManifestFourLaneService),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestStrictCrossLaneService {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBoundedColumnarBurst {
    max_consecutive_columnar_admissions: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFourLaneService {}

impl ManifestCrossLaneService {
    const fn into_policy(self) -> AutomaticCrossLaneServicePolicy {
        match self {
            Self::StrictPhysicalPriority(_) => {
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority
            }
            Self::BoundedColumnarBurst(ManifestBoundedColumnarBurst {
                max_consecutive_columnar_admissions,
            }) => AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                max_consecutive_columnar_admissions,
            },
            Self::BoundedFourLaneCycle(_) => AutomaticCrossLaneServicePolicy::BoundedFourLaneCycle,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAuthorization {
    local_plaintext: Option<ManifestPrincipal>,
    #[serde(default)]
    clients: Vec<ManifestClient>,
}

impl ManifestAuthorization {
    fn into_policy(
        self,
        transport: TransportKind,
        known_tables: &[TableId],
    ) -> Result<AuthorizationPolicy, AuthorizationConfigError> {
        let local_plaintext = self
            .local_plaintext
            .map(ManifestPrincipal::into_permissions);
        let clients = self
            .clients
            .into_iter()
            .map(|client| {
                Ok((
                    parse_certificate_sha256(&client.certificate_sha256)?,
                    client.principal.into_permissions(),
                ))
            })
            .collect::<Result<Vec<_>, AuthorizationConfigError>>()?;
        AuthorizationPolicy::new(transport, local_plaintext, clients, known_tables)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestClient {
    certificate_sha256: String,
    #[serde(flatten)]
    principal: ManifestPrincipal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPrincipal {
    #[serde(default)]
    schema_admin: bool,
    tables: Vec<ManifestTablePermissions>,
}

impl ManifestPrincipal {
    fn into_permissions(self) -> crate::authorization::PrincipalGrants {
        crate::authorization::PrincipalGrants {
            schema_admin: self.schema_admin,
            tables: self
                .tables
                .into_iter()
                .map(ManifestTablePermissions::into_permissions)
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTablePermissions {
    table_id: u64,
    #[serde(default)]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    transaction: bool,
    #[serde(default)]
    analyze: bool,
}

impl ManifestTablePermissions {
    const fn into_permissions(self) -> TablePermissions {
        TablePermissions::new(
            TableId(self.table_id),
            self.read,
            self.write,
            self.transaction,
            self.analyze,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTls {
    server_certificate: String,
    server_private_key: String,
    client_ca: String,
}

impl ManifestTls {
    fn resolve(self, manifest_directory: &Path) -> Result<TlsMaterialPaths, ManifestError> {
        Ok(TlsMaterialPaths {
            server_certificate: resolve_tls_path(
                manifest_directory,
                "server_certificate",
                self.server_certificate,
            )?,
            server_private_key: resolve_tls_path(
                manifest_directory,
                "server_private_key",
                self.server_private_key,
            )?,
            client_ca: resolve_tls_path(manifest_directory, "client_ca", self.client_ca)?,
        })
    }
}

fn resolve_tls_path(
    manifest_directory: &Path,
    field: &'static str,
    configured: String,
) -> Result<PathBuf, ManifestError> {
    let configured = PathBuf::from(configured);
    let resolved = if configured.is_absolute() {
        configured
    } else {
        manifest_directory.join(configured)
    };
    let resolved = resolved
        .canonicalize()
        .map_err(|source| ManifestError::TlsPath {
            field,
            path: resolved,
            source,
        })?;
    let metadata = std::fs::metadata(&resolved).map_err(|source| ManifestError::TlsPath {
        field,
        path: resolved.clone(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(ManifestError::TlsPathIsNotFile {
            field,
            path: resolved,
        });
    }
    Ok(resolved)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLimits {
    max_connections: Option<u64>,
    idle_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    max_result_rows: Option<u64>,
}

impl ManifestLimits {
    fn into_limits(self) -> Result<ServerLimits, ServerLimitsError> {
        ServerLimits::from_millis(
            self.max_connections
                .unwrap_or(DEFAULT_MAX_CONNECTIONS as u64),
            self.idle_timeout_ms
                .unwrap_or(DEFAULT_IDLE_TIMEOUT.as_millis() as u64),
            self.write_timeout_ms
                .unwrap_or(DEFAULT_WRITE_TIMEOUT.as_millis() as u64),
            self.max_result_rows
                .unwrap_or(DEFAULT_MAX_RESULT_ROWS as u64),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTable {
    path: String,
    id: u64,
    name: String,
    columns: Vec<ManifestColumn>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestColumn {
    id: u32,
    name: String,
    physical_type: ManifestPhysicalType,
    semantic_type: Option<String>,
    nullable: bool,
    primary_key: bool,
}

impl ManifestColumn {
    fn into_column(self) -> ColumnDef {
        let physical = self.physical_type.into_physical();
        let type_spec = match self.semantic_type {
            Some(name) => TypeSpec::Semantic { name, physical },
            None => TypeSpec::Physical(physical),
        };
        ColumnDef::new(ColumnId(self.id), self.name, type_spec)
            .nullable(self.nullable)
            .primary_key(self.primary_key)
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ManifestPhysicalType {
    Bool,
    Int64,
    Uint64,
    Text,
}

impl ManifestPhysicalType {
    const fn into_physical(self) -> PhysicalType {
        match self {
            Self::Bool => PhysicalType::Bool,
            Self::Int64 => PhysicalType::Int64,
            Self::Uint64 => PhysicalType::UInt64,
            Self::Text => PhysicalType::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use netbadb_core::Database;
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};
    use serde_json::json;

    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("netbadb-manifest-{name}-{}", std::process::id()))
    }

    fn users_table(semantic_name: &str) -> TableDef {
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: semantic_name.into(),
                        physical: PhysicalType::UInt64,
                    },
                )
                .primary_key(true),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )
    }

    fn create_heap(path: &Path) {
        Database::create(path, users_table("UserId"))
            .unwrap()
            .close()
            .unwrap();
    }

    fn manifest_json(listen: Option<&str>, path: &str, semantic_name: &str) -> String {
        let listen = listen.map_or_else(String::new, |listen| format!("\"listen\": \"{listen}\","));
        format!(
            r#"{{
                "version": 7,
                {listen}
                "authorization": {{
                    "local_plaintext": {{
                        "tables": [{{
                            "table_id": 1,
                            "read": true,
                            "write": true,
                            "transaction": true,
                            "analyze": true
                        }}]
                    }},
                    "clients": []
                }},
                "tables": [{{
                    "path": "{path}",
                    "id": 1,
                    "name": "users",
                    "columns": [
                        {{
                            "id": 1,
                            "name": "id",
                            "physical_type": "uint64",
                            "semantic_type": "{semantic_name}",
                            "nullable": false,
                            "primary_key": true
                        }},
                        {{
                            "id": 2,
                            "name": "name",
                            "physical_type": "text",
                            "semantic_type": null,
                            "nullable": false,
                            "primary_key": false
                        }}
                    ]
                }}]
            }}"#
        )
    }

    fn feedback_json() -> serde_json::Value {
        json!({
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
        })
    }

    fn physical_design_json() -> serde_json::Value {
        json!({
            "evidence_limits": {
                "max_index_candidates": 11,
                "max_columnar_candidates": 12,
                "max_query_shapes_per_candidate": 13,
                "max_columnar_columns_per_candidate": 14
            },
            "advisor_policy": {
                "index": {
                    "minimum_reports": 21,
                    "minimum_distinct_query_shapes": 22,
                    "minimum_actual_scan_work_units": 23,
                    "max_recommendations": 24
                },
                "columnar": {
                    "minimum_reports": 31,
                    "minimum_distinct_query_shapes": 32,
                    "minimum_actual_scan_work_units": 33,
                    "max_recommendations": 34
                }
            }
        })
    }

    fn driven_adaptive_json() -> serde_json::Value {
        json!({
            "mode": "driven",
            "feedback": feedback_json(),
            "host": { "tick_interval_ms": 10 },
            "scheduler_policy": {
                "minimum_ticks_between_runs": 1,
                "idle_retry_ticks": 8,
                "no_progress_retry_ticks": 8,
                "trial_retry_ticks": 8
            },
            "orchestration_envelope": {
                "max_steps": 4,
                "per_step_maintenance_budget": {
                    "max_work_units": 100_000,
                    "max_read_bytes": 16_777_216,
                    "max_write_bytes": 16_777_216,
                    "max_actions": 1
                },
                "run_maintenance_budget": {
                    "max_work_units": 400_000,
                    "max_read_bytes": 67_108_864,
                    "max_write_bytes": 67_108_864,
                    "max_actions": 4
                }
            },
            "scope": {
                "table_ids": [1],
                "calibration_classes": [
                    "seq_scan", "index_point", "index_range", "columnar"
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
                        "global_min_ratio": { "numerator": 2, "denominator": 4 },
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
                    "minimum_reclaimable_bytes": 1_048_576
                },
                "lsm_flush_policy": { "minimum_memtable_bytes": 0 },
                "lsm_compaction_policy": { "minimum_input_bytes": 0 },
                "columnar_compaction_policy": {
                    "minimum_delta_segments": 1,
                    "minimum_delta_bytes": 0
                },
                "cross_lane_service": { "mode": "bounded_four_lane_cycle" },
                "max_candidate_tables": 16,
                "max_calibration_classes": 4,
                "max_fairness_entries": 64
            }
        })
    }

    fn write_adaptive_manifest(manifest: &Path, adaptive: serde_json::Value) -> serde_json::Value {
        let mut value: serde_json::Value =
            serde_json::from_str(&manifest_json(Some("127.0.0.1:0"), "users.ndb", "UserId"))
                .unwrap();
        value["adaptive"] = adaptive;
        std::fs::write(manifest, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        value
    }

    fn wait_for_manifest_driver_tick(
        control: &crate::ServerAdaptiveControlHandle,
    ) -> crate::ServerAdaptiveStatus {
        for _ in 0..100 {
            let status = control.status().unwrap();
            if status
                .driver
                .is_some_and(|driver| driver.driver_tick_count != 0)
            {
                return status;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("manifest-derived driver did not receive a host tick");
    }

    #[test]
    fn relative_paths_and_full_table_defs_are_resolved_from_the_manifest() {
        let directory = test_directory("relative");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(directory.join("data")).unwrap();
        let heap = directory.join("data/users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, manifest_json(None, "data/users.ndb", "UserId")).unwrap();

        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.listen(), DEFAULT_LISTEN_ADDRESS);
        assert_eq!(config.limits(), ServerLimits::default());
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::Disabled);
        assert_eq!(config.tables().len(), 1);
        assert_eq!(config.tables()[0].path, heap.canonicalize().unwrap());
        assert_eq!(config.tables()[0].table, users_table("UserId"));
        assert_eq!(
            config.tables()[0].table.fingerprint().unwrap(),
            users_table("UserId").fingerprint().unwrap()
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn changing_only_v6_version_to_v7_preserves_runtime_behavior() {
        let directory = test_directory("v6-v7-migration");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let mut value: serde_json::Value =
            serde_json::from_str(&manifest_json(Some("127.0.0.1:0"), "users.ndb", "UserId"))
                .unwrap();
        value["adaptive"] = json!({"mode": "feedback_only", "feedback": feedback_json()});
        value["operator"] = json!({
            "unix_socket": "operator.sock",
            "io_timeout_ms": 1000
        });
        let v7 = serde_json::to_string(&value).unwrap();
        let mut v6_value = value.clone();
        v6_value["version"] = json!(6);
        let v6 = serde_json::to_string(&v6_value).unwrap();
        std::fs::write(&manifest, &v6).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::UnsupportedVersion(6))
        ));

        std::fs::write(&manifest, v7).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.listen(), "127.0.0.1:0".parse().unwrap());
        assert_eq!(config.limits(), ServerLimits::default());
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::FeedbackOnly);
        assert!(!config.physical_design_enabled());
        assert!(config.operator_config().is_some());
        assert_eq!(config.tables()[0].table, users_table("UserId"));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn adaptive_modes_map_exactly_without_hidden_defaults() {
        let directory = test_directory("adaptive-mapping");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");

        write_adaptive_manifest(
            &manifest,
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
        );
        let feedback = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(feedback.adaptive_mode(), ServerAdaptiveMode::FeedbackOnly);
        let ServerAdaptiveStartupMode::FeedbackOnly(feedback) = feedback.adaptive_mode else {
            panic!("expected feedback-only manifest mode");
        };
        assert_eq!(
            feedback.limits(),
            AdaptiveEvidencePoolLimits {
                max_target_windows: 16,
                workload_limits: AdaptiveWorkloadLimits::new(64, 8),
                max_calibration_epochs: 4,
                max_calibration_query_shapes: 64,
                max_calibration_plan_variants_per_shape: 8,
            }
        );

        write_adaptive_manifest(&manifest, driven_adaptive_json());
        let driven = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(driven.adaptive_mode(), ServerAdaptiveMode::Driven);
        let ServerAdaptiveStartupMode::Driven(driver) = driven.adaptive_mode else {
            panic!("expected driven manifest mode");
        };
        assert_eq!(driver.tick_interval(), Duration::from_millis(10));
        assert_eq!(
            driver.scheduler_policy(),
            AutomaticSchedulerPolicy::new(1, 8, 8, 8).unwrap()
        );
        assert_eq!(driver.orchestration_envelope().max_steps, 4);
        assert_eq!(
            driver.orchestration_envelope().per_step_maintenance_budget,
            MaintenanceBudget::new(100_000, 16_777_216, 16_777_216, 1)
        );
        assert_eq!(driver.table_ids(), &[TableId(1)]);
        assert_eq!(
            driver.calibration_classes(),
            &[
                PlannerCalibrationClass::SeqScan,
                PlannerCalibrationClass::IndexPoint,
                PlannerCalibrationClass::IndexRange,
                PlannerCalibrationClass::Columnar,
            ]
        );
        assert_eq!(
            driver
                .automatic_policy()
                .safe_mode
                .planner_calibration_policy
                .global_min_ratio,
            CalibrationRatio::HALF
        );
        assert_eq!(
            driver.automatic_policy().cross_lane_service,
            AutomaticCrossLaneServicePolicy::BoundedFourLaneCycle
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn physical_design_maps_exactly_and_preserves_zero_semantics() {
        let directory = test_directory("physical-design-mapping");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let mut value: serde_json::Value =
            serde_json::from_str(&manifest_json(None, "users.ndb", "UserId")).unwrap();
        assert!(
            !ServerConfig::from_manifest_path({
                std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
                &manifest
            })
            .unwrap()
            .physical_design_enabled()
        );

        value["physical_design"] = physical_design_json();
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let design = *config.physical_design_config().unwrap();
        assert_eq!(
            design.evidence_limits(),
            PhysicalDesignEvidenceLimits {
                max_index_candidates: 11,
                max_columnar_candidates: 12,
                max_query_shapes_per_candidate: 13,
                max_columnar_columns_per_candidate: 14,
            }
        );
        assert_eq!(
            design.advisor_policy(),
            PhysicalDesignAdvisorPolicy {
                index: PhysicalDesignRecommendationPolicy {
                    minimum_reports: 21,
                    minimum_distinct_query_shapes: 22,
                    minimum_actual_scan_work_units: 23,
                    max_recommendations: 24,
                },
                columnar: PhysicalDesignRecommendationPolicy {
                    minimum_reports: 31,
                    minimum_distinct_query_shapes: 32,
                    minimum_actual_scan_work_units: 33,
                    max_recommendations: 34,
                },
            }
        );

        value["physical_design"] = json!({
            "evidence_limits": {
                "max_index_candidates": 0,
                "max_columnar_candidates": 0,
                "max_query_shapes_per_candidate": 0,
                "max_columnar_columns_per_candidate": 0
            },
            "advisor_policy": {
                "index": {
                    "minimum_reports": 0,
                    "minimum_distinct_query_shapes": 0,
                    "minimum_actual_scan_work_units": 0,
                    "max_recommendations": 0
                },
                "columnar": {
                    "minimum_reports": 0,
                    "minimum_distinct_query_shapes": 0,
                    "minimum_actual_scan_work_units": 0,
                    "max_recommendations": 0
                }
            }
        });
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let zero = *ServerConfig::from_manifest_path(&manifest)
            .unwrap()
            .physical_design_config()
            .unwrap();
        assert_eq!(zero.evidence_limits().max_index_candidates, 0);
        assert_eq!(zero.advisor_policy().index.minimum_reports, 0);
        assert_eq!(zero.advisor_policy().columnar.max_recommendations, 0);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn physical_design_json_is_strict_complete_and_non_null() {
        let directory = test_directory("physical-design-strict");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let base: serde_json::Value =
            serde_json::from_str(&manifest_json(None, "users.ndb", "UserId")).unwrap();
        for physical_design in [
            json!(null),
            json!({}),
            json!({"evidence_limits": {}, "advisor_policy": {}}),
            {
                let mut value = physical_design_json();
                value["unknown"] = json!(true);
                value
            },
            {
                let mut value = physical_design_json();
                value["evidence_limits"]["unknown"] = json!(true);
                value
            },
            {
                let mut value = physical_design_json();
                value["advisor_policy"]["index"]["unknown"] = json!(true);
                value
            },
        ] {
            let mut value = base.clone();
            value["physical_design"] = physical_design;
            std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::Json(_))
            ));
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn operator_is_optional_requires_a_managed_runtime_and_resolves_from_manifest_directory() {
        let directory = test_directory("operator-config");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(directory.join("run")).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");

        let disabled: serde_json::Value =
            serde_json::from_str(&manifest_json(None, "users.ndb", "UserId")).unwrap();
        std::fs::write(&manifest, serde_json::to_vec(&disabled).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::Disabled);
        assert!(config.operator_config().is_none());

        let mut invalid = disabled.clone();
        invalid["operator"] = json!({
            "unix_socket": "run/operator.sock",
            "io_timeout_ms": 5000
        });
        std::fs::write(&manifest, serde_json::to_vec(&invalid).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::OperatorRequiresManagedRuntime)
        ));

        for adaptive in [
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
            driven_adaptive_json(),
        ] {
            let mut value = disabled.clone();
            value["adaptive"] = adaptive;
            value["operator"] = json!({
                "unix_socket": "run/operator.sock",
                "io_timeout_ms": 5000
            });
            std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
            let config = ServerConfig::from_manifest_path(&manifest).unwrap();
            let operator = config.operator_config().unwrap();
            assert_eq!(
                operator.unix_socket(),
                directory.canonicalize().unwrap().join("run/operator.sock")
            );
            assert_eq!(operator.io_timeout(), Duration::from_millis(5000));
            assert!(!operator.unix_socket().exists());
        }

        let mut design_only = disabled.clone();
        design_only["physical_design"] = physical_design_json();
        design_only["operator"] = json!({
            "unix_socket": "run/operator.sock",
            "io_timeout_ms": 5000
        });
        std::fs::write(&manifest, serde_json::to_vec(&design_only).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::Disabled);
        assert!(config.physical_design_enabled());
        assert!(config.operator_config().is_some());

        let mut both = design_only;
        both["adaptive"] = json!({"mode": "feedback_only", "feedback": feedback_json()});
        std::fs::write(&manifest, serde_json::to_vec(&both).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::FeedbackOnly);
        assert!(config.physical_design_enabled());
        assert!(config.operator_config().is_some());

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn operator_json_is_strict_non_null_and_requires_an_existing_parent() {
        let directory = test_directory("operator-strict");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let base = write_adaptive_manifest(
            &manifest,
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
        );

        for operator in [
            json!(null),
            json!({"unix_socket": "operator.sock"}),
            json!({"unix_socket": "operator.sock", "io_timeout_ms": 0}),
            json!({
                "unix_socket": "operator.sock",
                "io_timeout_ms": 1,
                "token": "forbidden"
            }),
        ] {
            let mut value = base.clone();
            value["operator"] = operator;
            std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::Json(_) | ManifestError::OperatorConfig(_))
            ));
        }

        let mut value = base;
        value["operator"] = json!({
            "unix_socket": "missing/operator.sock",
            "io_timeout_ms": 1
        });
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::OperatorSocketParent { .. })
        ));

        std::fs::write(directory.join("not-a-directory"), b"file").unwrap();
        let mut value =
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(&manifest).unwrap())
                .unwrap();
        value["operator"]["unix_socket"] = json!("not-a-directory/operator.sock");
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::OperatorSocketParentNotDirectory(_))
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn live_feedback_operator_status_and_conditional_rotation_are_retry_safe() {
        use std::io::Write;
        use std::net::Shutdown;
        use std::os::unix::net::UnixStream;

        let directory = test_directory("operator-live");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let socket = PathBuf::from(format!(
            "/tmp/netbadb-manifest-op-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&socket);
        let mut value = write_adaptive_manifest(
            &manifest,
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
        );
        value["operator"] = json!({
            "unix_socket": socket,
            "io_timeout_ms": 1000
        });
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();

        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let operator_config = config.operator_config().unwrap().clone();
        let server = crate::TcpServer::new(config).start().unwrap();
        let client = crate::ServerOperatorClient::new(&operator_config);
        let before = client.status().unwrap();
        let adaptive = before.adaptive.as_ref().unwrap();
        assert_eq!(adaptive.mode, crate::OperatorAdaptiveModeV2::FeedbackOnly);
        assert_eq!(adaptive.feedback.window_epoch, 0);
        assert!(adaptive.driver.is_none());
        assert!(before.physical_design.is_none());
        for _ in 0..100 {
            assert_eq!(client.status().unwrap(), before);
        }

        let payload = serde_json::to_vec(&json!({
            "request_id": 42,
            "operation": {
                "type": "rotate_evidence",
                "expected_window_epoch": 0
            }
        }))
        .unwrap();
        let mut frame = b"NBOP\0\x02\0\0".to_vec();
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        let mut lost_response = UnixStream::connect(operator_config.unix_socket()).unwrap();
        lost_response.write_all(&frame).unwrap();
        lost_response.shutdown(Shutdown::Both).unwrap();
        for _ in 0..100 {
            if client
                .status()
                .unwrap()
                .adaptive
                .unwrap()
                .feedback
                .window_epoch
                == 1
            {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(
            client
                .status()
                .unwrap()
                .adaptive
                .unwrap()
                .feedback
                .window_epoch,
            1
        );
        assert!(matches!(
            client.rotate_evidence(0),
            Err(crate::OperatorClientError::Remote(
                crate::OperatorRemoteErrorV2 {
                    code: crate::OperatorErrorCodeV2::EvidenceWindowChanged,
                    ..
                }
            ))
        ));
        assert_eq!(
            client
                .status()
                .unwrap()
                .adaptive
                .unwrap()
                .feedback
                .window_epoch,
            1
        );
        assert!(matches!(
            client.reset_faulted_scheduler(),
            Err(crate::OperatorClientError::Remote(
                crate::OperatorRemoteErrorV2 {
                    code: crate::OperatorErrorCodeV2::DriverNotEnabled,
                    ..
                }
            ))
        ));

        server.shutdown().unwrap();
        assert!(!operator_config.unix_socket().exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn operator_bind_failure_stops_native_and_postgres_workers_without_removing_path() {
        let directory = test_directory("operator-startup-failure");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let heap = directory.join("users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");
        let socket = PathBuf::from(format!(
            "/tmp/netbadb-existing-op-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&socket);
        std::fs::write(&socket, b"must survive").unwrap();
        let mut value = write_adaptive_manifest(
            &manifest,
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
        );
        value["operator"] = json!({
            "unix_socket": socket,
            "io_timeout_ms": 100
        });
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();

        let native =
            crate::TcpServer::new(ServerConfig::from_manifest_path(&manifest).unwrap()).start();
        assert!(matches!(
            native,
            Err(crate::TcpServerError::Operator(
                crate::ServerOperatorError::PathExists(_)
            ))
        ));
        assert_eq!(std::fs::read(&socket).unwrap(), b"must survive");
        Database::open(&heap, users_table("UserId"))
            .unwrap()
            .close()
            .unwrap();

        let postgres =
            crate::PostgresTcpServer::new(ServerConfig::from_manifest_path(&manifest).unwrap())
                .start();
        assert!(matches!(
            postgres,
            Err(crate::PostgresTcpServerError::Operator(
                crate::ServerOperatorError::PathExists(_)
            ))
        ));
        assert_eq!(std::fs::read(&socket).unwrap(), b"must survive");
        Database::open(&heap, users_table("UserId"))
            .unwrap()
            .close()
            .unwrap();

        std::fs::remove_file(socket).unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn documented_v7_driven_example_is_a_golden_manifest() {
        let directory = test_directory("documented-v7-example");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(directory.join("run")).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let document = include_str!("../../../docs/server-manifest-v7.md");
        let example = document
            .split_once("```json\n")
            .and_then(|(_, remainder)| remainder.split_once("\n```"))
            .map(|(example, _)| example)
            .expect("v7 documentation contains a JSON example")
            .replace("127.0.0.1:7878", "127.0.0.1:0")
            .replace("data/users.ndb", "users.ndb");
        std::fs::write(&manifest, example).unwrap();

        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.adaptive_mode(), ServerAdaptiveMode::Driven);
        assert_eq!(config.tables().len(), 1);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn adaptive_json_is_strict_tagged_and_non_null() {
        let directory = test_directory("adaptive-json-strict");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");

        let invalid_values = [
            json!(null),
            json!({"mode": "disabled"}),
            json!({"mode": "autonomous"}),
            json!({"mode": "feedback_only"}),
            json!({
                "mode": "feedback_only",
                "feedback": feedback_json(),
                "magic_auto_tune": true
            }),
        ];
        for invalid in invalid_values {
            write_adaptive_manifest(&manifest, invalid);
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::Json(_))
            ));
        }

        let mut missing_policy = driven_adaptive_json();
        missing_policy
            .as_object_mut()
            .unwrap()
            .remove("automatic_policy");
        write_adaptive_manifest(&manifest, missing_policy);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        let mut unknown_nested = driven_adaptive_json();
        unknown_nested["feedback"]["limits"]["workload"]["samples"] = json!(4);
        write_adaptive_manifest(&manifest, unknown_nested);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        let mut unknown_class = driven_adaptive_json();
        unknown_class["scope"]["calibration_classes"] = json!(["seq"]);
        write_adaptive_manifest(&manifest, unknown_class);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        let mut variant_field = driven_adaptive_json();
        variant_field["automatic_policy"]["cross_lane_service"] = json!({
            "mode": "bounded_four_lane_cycle",
            "max_consecutive_columnar_admissions": 3
        });
        write_adaptive_manifest(&manifest, variant_field);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn adaptive_semantics_reuse_typed_runtime_validation() {
        let directory = test_directory("adaptive-semantics");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");

        let mut invalid = driven_adaptive_json();
        invalid["automatic_policy"]["safe_mode"]["planner_calibration_policy"]["global_min_ratio"]
            ["denominator"] = json!(0);
        write_adaptive_manifest(&manifest, invalid);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::CalibrationRatio {
                source: CalibrationRatioError::ZeroDenominator,
                ..
            })
        ));

        for field in [
            "global_min_ratio",
            "maximum_step_up_ratio",
            "maximum_step_down_ratio",
        ] {
            let mut invalid = driven_adaptive_json();
            let planner =
                &mut invalid["automatic_policy"]["safe_mode"]["planner_calibration_policy"];
            if field == "global_min_ratio" {
                planner[field] = json!({"numerator": 3, "denominator": 1});
            } else {
                planner[field] = json!({"numerator": 1, "denominator": 2});
            }
            write_adaptive_manifest(&manifest, invalid);
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::AdaptiveDriverConfig(
                    ServerAdaptiveDriverConfigError::InvalidPlannerCalibrationPolicy
                ))
            ));
        }

        let mut invalid = driven_adaptive_json();
        invalid["scheduler_policy"]["minimum_ticks_between_runs"] = json!(4);
        invalid["scheduler_policy"]["idle_retry_ticks"] = json!(2);
        write_adaptive_manifest(&manifest, invalid);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::AdaptiveSchedulerPolicy(_))
        ));

        let mut invalid = driven_adaptive_json();
        invalid["host"]["tick_interval_ms"] = json!(0);
        write_adaptive_manifest(&manifest, invalid);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::AdaptiveDriverConfig(
                ServerAdaptiveDriverConfigError::ZeroTickInterval
            ))
        ));

        for steps in [0, 65] {
            let mut invalid = driven_adaptive_json();
            invalid["orchestration_envelope"]["max_steps"] = json!(steps);
            write_adaptive_manifest(&manifest, invalid);
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::AdaptiveDriverConfig(
                    ServerAdaptiveDriverConfigError::ZeroOrchestrationSteps
                        | ServerAdaptiveDriverConfigError::OrchestrationStepLimitExceeded { .. }
                ))
            ));
        }

        for (path, expected) in [
            (
                vec!["scope", "table_ids"],
                ServerAdaptiveDriverConfigError::DuplicateTableId(TableId(1)),
            ),
            (
                vec!["scope", "calibration_classes"],
                ServerAdaptiveDriverConfigError::DuplicateCalibrationClass(
                    PlannerCalibrationClass::SeqScan,
                ),
            ),
        ] {
            let mut invalid = driven_adaptive_json();
            if path[1] == "table_ids" {
                invalid[path[0]][path[1]] = json!([1, 1]);
            } else {
                invalid[path[0]][path[1]] = json!(["seq_scan", "seq_scan"]);
            }
            write_adaptive_manifest(&manifest, invalid);
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::AdaptiveDriverConfig(actual)) if actual == expected
            ));
        }

        let mut invalid = driven_adaptive_json();
        invalid["automatic_policy"]["cross_lane_service"] = json!({
            "mode": "bounded_columnar_burst",
            "max_consecutive_columnar_admissions": 0
        });
        write_adaptive_manifest(&manifest, invalid);
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::AdaptiveDriverConfig(
                ServerAdaptiveDriverConfigError::InvalidCrossLaneServicePolicy
            ))
        ));

        for (enabled, policy, expected) in [
            (
                "allow_columnar_compaction",
                "columnar_compaction_policy",
                ServerAdaptiveDriverConfigError::InvalidColumnarCompactionPolicy,
            ),
            (
                "allow_change_stream_gc",
                "change_stream_gc_policy",
                ServerAdaptiveDriverConfigError::InvalidChangeStreamGcPolicy,
            ),
        ] {
            let mut invalid = driven_adaptive_json();
            invalid["automatic_policy"][enabled] = json!(true);
            let object = invalid["automatic_policy"][policy].as_object_mut().unwrap();
            for value in object.values_mut() {
                *value = json!(0);
            }
            write_adaptive_manifest(&manifest, invalid);
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::AdaptiveDriverConfig(actual)) if actual == expected
            ));
        }

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_modes_start_both_transports_and_builder_override_wins() {
        let directory = test_directory("adaptive-startup");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        write_adaptive_manifest(&manifest, driven_adaptive_json());

        let driven_config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let ServerAdaptiveStartupMode::Driven(driver) = driven_config.adaptive_mode.clone() else {
            panic!("expected driven config");
        };
        let native = crate::TcpServer::new(driven_config.clone())
            .start()
            .unwrap();
        assert_eq!(
            native.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::Driven
        );
        assert_eq!(
            wait_for_manifest_driver_tick(&native.adaptive_control()).mode,
            ServerAdaptiveMode::Driven
        );
        native.shutdown().unwrap();

        let postgres = crate::PostgresTcpServer::new(driven_config.clone())
            .start()
            .unwrap();
        assert_eq!(
            postgres.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::Driven
        );
        assert_eq!(
            wait_for_manifest_driver_tick(&postgres.adaptive_control()).mode,
            ServerAdaptiveMode::Driven
        );
        postgres.shutdown().unwrap();

        let feedback_override = ServerAdaptiveFeedbackConfig::new(AdaptiveEvidencePoolLimits {
            max_target_windows: 1,
            workload_limits: AdaptiveWorkloadLimits::new(1, 1),
            max_calibration_epochs: 1,
            max_calibration_query_shapes: 1,
            max_calibration_plan_variants_per_shape: 1,
        });
        let native = crate::TcpServer::new(driven_config.clone())
            .with_adaptive_feedback(feedback_override)
            .start()
            .unwrap();
        let status = native.adaptive_control().status().unwrap();
        assert_eq!(status.mode, ServerAdaptiveMode::FeedbackOnly);
        assert!(status.feedback.is_some());
        assert!(status.driver.is_none());
        native.shutdown().unwrap();
        let postgres = crate::PostgresTcpServer::new(driven_config.clone())
            .with_adaptive_feedback(feedback_override)
            .start()
            .unwrap();
        let status = postgres.adaptive_control().status().unwrap();
        assert_eq!(status.mode, ServerAdaptiveMode::FeedbackOnly);
        assert!(status.feedback.is_some());
        assert!(status.driver.is_none());
        postgres.shutdown().unwrap();

        write_adaptive_manifest(
            &manifest,
            json!({"mode": "feedback_only", "feedback": feedback_json()}),
        );
        let feedback_config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let native = crate::TcpServer::new(feedback_config.clone())
            .start()
            .unwrap();
        let status = native.adaptive_control().status().unwrap();
        assert_eq!(status.mode, ServerAdaptiveMode::FeedbackOnly);
        assert!(status.driver.is_none());
        native.shutdown().unwrap();
        let postgres = crate::PostgresTcpServer::new(feedback_config.clone())
            .start()
            .unwrap();
        let status = postgres.adaptive_control().status().unwrap();
        assert_eq!(status.mode, ServerAdaptiveMode::FeedbackOnly);
        assert!(status.driver.is_none());
        postgres.shutdown().unwrap();

        let native = crate::TcpServer::new(feedback_config.clone())
            .with_adaptive_driver((*driver).clone())
            .start()
            .unwrap();
        assert_eq!(
            native.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::Driven
        );
        native.shutdown().unwrap();
        let postgres = crate::PostgresTcpServer::new(feedback_config)
            .with_adaptive_driver((*driver).clone())
            .start()
            .unwrap();
        assert_eq!(
            postgres.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::Driven
        );
        postgres.shutdown().unwrap();

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_physical_design_starts_both_transports_and_builder_override_is_independent() {
        let directory = test_directory("physical-design-startup");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let mut value: serde_json::Value =
            serde_json::from_str(&manifest_json(Some("127.0.0.1:0"), "users.ndb", "UserId"))
                .unwrap();
        value["physical_design"] = physical_design_json();
        value["adaptive"] = json!({"mode": "feedback_only", "feedback": feedback_json()});
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();

        let native = crate::TcpServer::new(config.clone()).start().unwrap();
        assert_eq!(
            native
                .physical_design_control()
                .status()
                .unwrap()
                .evidence
                .limits
                .max_index_candidates,
            11
        );
        assert_eq!(
            native.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::FeedbackOnly
        );
        native.shutdown().unwrap();

        let postgres = crate::PostgresTcpServer::new(config.clone())
            .start()
            .unwrap();
        assert_eq!(
            postgres
                .physical_design_control()
                .status()
                .unwrap()
                .evidence
                .limits
                .max_columnar_candidates,
            12
        );
        postgres.shutdown().unwrap();

        let replacement = ServerPhysicalDesignAdvisorConfig::new(
            PhysicalDesignEvidenceLimits {
                max_index_candidates: 91,
                max_columnar_candidates: 92,
                max_query_shapes_per_candidate: 93,
                max_columnar_columns_per_candidate: 94,
            },
            PhysicalDesignAdvisorPolicy {
                index: PhysicalDesignRecommendationPolicy {
                    minimum_reports: 1,
                    minimum_distinct_query_shapes: 1,
                    minimum_actual_scan_work_units: 1,
                    max_recommendations: 1,
                },
                columnar: PhysicalDesignRecommendationPolicy {
                    minimum_reports: 2,
                    minimum_distinct_query_shapes: 2,
                    minimum_actual_scan_work_units: 2,
                    max_recommendations: 2,
                },
            },
        );
        let mut driven_value = value;
        driven_value["adaptive"] = driven_adaptive_json();
        std::fs::write(&manifest, serde_json::to_vec(&driven_value).unwrap()).unwrap();
        let driven_config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let ServerAdaptiveStartupMode::Driven(driver) = driven_config.adaptive_mode else {
            panic!("expected driven manifest mode");
        };
        let native = crate::TcpServer::new(config.clone())
            .with_adaptive_driver(*driver)
            .with_physical_design_advisor(replacement)
            .start()
            .unwrap();
        assert_eq!(
            native
                .physical_design_control()
                .status()
                .unwrap()
                .evidence
                .limits
                .max_index_candidates,
            91
        );
        assert_eq!(
            native.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::Driven
        );
        native.shutdown().unwrap();

        let postgres = crate::PostgresTcpServer::new(config)
            .with_physical_design_advisor(replacement)
            .start()
            .unwrap();
        assert_eq!(
            postgres
                .physical_design_control()
                .status()
                .unwrap()
                .evidence
                .limits
                .max_index_candidates,
            91
        );
        assert_eq!(
            postgres.adaptive_control().status().unwrap().mode,
            ServerAdaptiveMode::FeedbackOnly
        );
        postgres.shutdown().unwrap();

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unknown_adaptive_table_fails_before_worker_readiness() {
        let directory = test_directory("adaptive-unknown-table");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let mut adaptive = driven_adaptive_json();
        adaptive["scope"]["table_ids"] = json!([9]);
        write_adaptive_manifest(&manifest, adaptive);
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert!(matches!(
            crate::TcpServer::new(config).start(),
            Err(crate::TcpServerError::AdaptiveConfig(
                ServerAdaptiveDriverConfigError::UnknownTableId(TableId(9))
            ))
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_schema_is_an_expectation_checked_when_storage_opens() {
        let directory = test_directory("round16-schema-expectation");
        std::fs::create_dir(&directory).unwrap();
        let heap = directory.join("users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, manifest_json(None, "users.ndb", "DifferentId")).unwrap();

        // Config parsing validates definitions/paths, not the heap fingerprint.
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.tables()[0].table, users_table("DifferentId"));
        let entries = config
            .tables()
            .iter()
            .map(|entry| (entry.path.clone(), entry.table.clone()))
            .collect();
        assert!(matches!(
            Database::open_tables_with_expectation(entries),
            Err(netbadb_core::DatabaseError::Storage(
                netbadb_storage::StorageError::SchemaMismatch { .. }
            ))
        ));
        Database::open(&heap, users_table("UserId"))
            .unwrap()
            .close()
            .unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn stale_manifest_subset_opens_complete_persisted_catalog() {
        let directory = test_directory("round17-subset");
        std::fs::create_dir(&directory).unwrap();
        let users = users_table("UserId");
        let extra = TableDef::new(TableId(9), "future_extra", users.columns.clone());
        Database::create_tables(vec![
            (directory.join("users.ndb"), users.clone()),
            (directory.join("extra.ndb"), extra.clone()),
        ])
        .unwrap()
        .close()
        .unwrap();
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, manifest_json(None, "users.ndb", "UserId")).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let entries = config
            .tables()
            .iter()
            .map(|t| (t.path.clone(), t.table.clone()))
            .collect();
        let database = Database::open_tables_with_expectation(entries).unwrap();
        assert_eq!(database.schema().tables(), &[users, extra]);
        assert_eq!(database.inspect_catalog().unwrap().tables.len(), 2);
        database.close().unwrap();
        // Same name but different TableId cannot redirect an authorization grant.
        let mut bad: serde_json::Value =
            serde_json::from_str(&manifest_json(None, "users.ndb", "UserId")).unwrap();
        bad["tables"][0]["id"] = json!(2);
        bad["authorization"]["local_plaintext"]["tables"][0]["table_id"] = json!(2);
        std::fs::write(&manifest, serde_json::to_vec(&bad).unwrap()).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        let entries = config
            .tables()
            .iter()
            .map(|t| (t.path.clone(), t.table.clone()))
            .collect();
        assert!(matches!(
            Database::open_tables_with_expectation(entries),
            Err(netbadb_core::DatabaseError::Storage(
                netbadb_storage::StorageError::TableIdMismatch { .. }
            ))
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unknown_versions_fields_remote_listeners_and_missing_paths() {
        let directory = test_directory("invalid");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let manifest = directory.join("server.json");

        for version in [1, 2, 3, 4, 5, 6, 8] {
            std::fs::write(&manifest, format!(r#"{{"version":{version},"tables":[]}}"#)).unwrap();
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::UnsupportedVersion(actual)) if actual == version
            ));
        }

        std::fs::write(
            &manifest,
            r#"{"version":7,"unexpected":true,"authorization":{"local_plaintext":{"tables":[{"table_id":1,"read":true}]}},"tables":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::write(
            &manifest,
            manifest_json(Some("0.0.0.0:7878"), "missing.ndb", "UserId"),
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::RemoteListenRequiresMutualTls(_))
        ));

        std::fs::write(
            &manifest,
            manifest_json(Some("127.0.0.1:0"), "missing.ndb", "UserId"),
        )
        .unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::TablePath { .. })
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn limits_are_partial_strict_and_bounded() {
        let directory = test_directory("limits");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let heap = directory.join("users.ndb");
        create_heap(&heap);
        let manifest = directory.join("server.json");

        let source = manifest_json(None, "users.ndb", "UserId").replace(
            "\"authorization\":",
            "\"limits\": {\"max_connections\": 2, \"idle_timeout_ms\": 250, \"max_result_rows\": 3}, \"authorization\":",
        );
        std::fs::write(&manifest, source).unwrap();
        let config = ServerConfig::from_manifest_path(&manifest).unwrap();
        assert_eq!(config.limits().max_connections(), 2);
        assert_eq!(config.limits().idle_timeout(), Duration::from_millis(250));
        assert_eq!(config.limits().write_timeout(), DEFAULT_WRITE_TIMEOUT);
        assert_eq!(config.limits().max_result_rows(), 3);

        let invalid = manifest_json(None, "users.ndb", "UserId").replace(
            "\"authorization\":",
            "\"limits\": {\"max_connections\": 0}, \"authorization\":",
        );
        std::fs::write(&manifest, invalid).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Limits(error)) if error.field() == "max_connections"
        ));

        let unknown = manifest_json(None, "users.ndb", "UserId").replace(
            "\"authorization\":",
            "\"limits\": {\"connection_limit\": 2}, \"authorization\":",
        );
        std::fs::write(&manifest, unknown).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_v7_authorization_is_required_strict_and_schema_bound() {
        let directory = test_directory("authorization");
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        create_heap(&directory.join("users.ndb"));
        let manifest = directory.join("server.json");
        let base = manifest_json(None, "users.ndb", "UserId");

        let mut missing: serde_json::Value = serde_json::from_str(&base).unwrap();
        missing.as_object_mut().unwrap().remove("authorization");
        std::fs::write(&manifest, serde_json::to_vec(&missing).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        let mut no_local: serde_json::Value = serde_json::from_str(&base).unwrap();
        no_local["authorization"] = json!({"clients": []});
        std::fs::write(&manifest, serde_json::to_vec(&no_local).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Authorization(
                AuthorizationConfigError::PlaintextLocalPolicyRequired
            ))
        ));

        let mut unknown_table: serde_json::Value = serde_json::from_str(&base).unwrap();
        unknown_table["authorization"]["local_plaintext"]["tables"] =
            json!([{"table_id": 9, "read": true}]);
        std::fs::write(&manifest, serde_json::to_vec(&unknown_table).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Authorization(
                AuthorizationConfigError::UnknownTable {
                    table_id: TableId(9)
                }
            ))
        ));

        let mut duplicate_table: serde_json::Value = serde_json::from_str(&base).unwrap();
        duplicate_table["authorization"]["local_plaintext"]["tables"] = json!([
            {"table_id": 1, "read": true},
            {"table_id": 1, "write": true}
        ]);
        std::fs::write(&manifest, serde_json::to_vec(&duplicate_table).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Authorization(
                AuthorizationConfigError::DuplicateTableGrant {
                    table_id: TableId(1)
                }
            ))
        ));

        for fingerprint in ["ab".to_owned(), format!("{}z", "0".repeat(63))] {
            let mut invalid: serde_json::Value = serde_json::from_str(&base).unwrap();
            invalid["authorization"]["clients"] = json!([{
                "certificate_sha256": fingerprint,
                "tables": [{"table_id": 1, "read": true}]
            }]);
            std::fs::write(&manifest, serde_json::to_vec(&invalid).unwrap()).unwrap();
            assert!(matches!(
                ServerConfig::from_manifest_path(&manifest),
                Err(ManifestError::Authorization(
                    AuthorizationConfigError::InvalidFingerprintLength { .. }
                        | AuthorizationConfigError::InvalidFingerprintHex { .. }
                ))
            ));
        }

        let mut unknown_field: serde_json::Value = serde_json::from_str(&base).unwrap();
        unknown_field["authorization"]["local_plaintext"]["tables"][0]["select"] = json!(true);
        std::fs::write(&manifest, serde_json::to_vec(&unknown_field).unwrap()).unwrap();
        assert!(matches!(
            ServerConfig::from_manifest_path(&manifest),
            Err(ManifestError::Json(_))
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod schema_admin_tests {
    use super::*;
    use crate::{AuthenticatedClientIdentity, ClientIdentity};

    #[test]
    fn old_and_explicit_local_and_certificate_principals_preserve_schema_default_deny() {
        for explicit in [false, true] {
            let extra = if explicit {
                ",\"schema_admin\":true"
            } else {
                ""
            };
            let principal = format!("{{\"tables\":[{{\"table_id\":1,\"read\":true}}]{extra}}}");
            let local: ManifestAuthorization =
                serde_json::from_str(&format!("{{\"local_plaintext\":{principal}}}")).unwrap();
            let policy = local
                .into_policy(TransportKind::PlaintextLoopback, &[TableId(1)])
                .unwrap();
            assert_eq!(
                policy
                    .admit(&ClientIdentity::LocalPlaintext)
                    .unwrap()
                    .schema_admin(),
                explicit
            );
            let certificate = format!(
                "{{\"certificate_sha256\":\"{}\",\"tables\":[{{\"table_id\":1,\"read\":true}}]{extra}}}",
                "01".repeat(32)
            );
            let tls: ManifestAuthorization =
                serde_json::from_str(&format!("{{\"clients\":[{certificate}]}}")).unwrap();
            let policy = tls
                .into_policy(TransportKind::MutualTls, &[TableId(1)])
                .unwrap();
            assert_eq!(
                policy
                    .admit(&ClientIdentity::MutualTls(
                        AuthenticatedClientIdentity::from_certificate_sha256_for_test([1; 32])
                    ))
                    .unwrap()
                    .schema_admin(),
                explicit
            );
        }
    }
}
