use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Default upper bound for concurrently admitted TCP connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 128;
/// Default socket read timeout used to disconnect idle or stalled clients.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Default socket write timeout used for protocol responses.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Default maximum number of rows in one materialized query result.
pub const DEFAULT_MAX_RESULT_ROWS: usize = 100_000;

/// Largest accepted manifest value for concurrent connections.
pub const MAX_CONFIGURED_CONNECTIONS: usize = 65_536;
/// Largest accepted socket read or write timeout.
pub const MAX_SOCKET_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
/// Largest accepted manifest value for rows in one query result.
pub const MAX_CONFIGURED_RESULT_ROWS: usize = 10_000_000;

const MIN_SOCKET_TIMEOUT: Duration = Duration::from_millis(1);

/// Per-session policy applied by the transport-neutral protocol session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionPolicy {
    max_result_rows: usize,
}

impl SessionPolicy {
    /// Creates a validated policy for a nonzero, bounded result-row limit.
    pub fn new(max_result_rows: usize) -> Result<Self, ServerLimitsError> {
        validate_usize(
            "max_result_rows",
            max_result_rows,
            1,
            MAX_CONFIGURED_RESULT_ROWS,
        )?;
        Ok(Self { max_result_rows })
    }

    /// Returns the maximum rows allowed in one materialized query result.
    #[must_use]
    pub const fn max_result_rows(self) -> usize {
        self.max_result_rows
    }
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        }
    }
}

/// Validated process-level admission, socket, and session limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerLimits {
    max_connections: usize,
    idle_timeout: Duration,
    write_timeout: Duration,
    session_policy: SessionPolicy,
}

impl ServerLimits {
    /// Creates validated server limits.
    pub fn new(
        max_connections: usize,
        idle_timeout: Duration,
        write_timeout: Duration,
        max_result_rows: usize,
    ) -> Result<Self, ServerLimitsError> {
        validate_usize(
            "max_connections",
            max_connections,
            1,
            MAX_CONFIGURED_CONNECTIONS,
        )?;
        validate_timeout("idle_timeout_ms", idle_timeout)?;
        validate_timeout("write_timeout_ms", write_timeout)?;
        Ok(Self {
            max_connections,
            idle_timeout,
            write_timeout,
            session_policy: SessionPolicy::new(max_result_rows)?,
        })
    }

    pub(crate) fn from_millis(
        max_connections: u64,
        idle_timeout_ms: u64,
        write_timeout_ms: u64,
        max_result_rows: u64,
    ) -> Result<Self, ServerLimitsError> {
        validate_u64(
            "max_connections",
            max_connections,
            1,
            MAX_CONFIGURED_CONNECTIONS as u64,
        )?;
        validate_u64(
            "idle_timeout_ms",
            idle_timeout_ms,
            1,
            MAX_SOCKET_TIMEOUT.as_millis() as u64,
        )?;
        validate_u64(
            "write_timeout_ms",
            write_timeout_ms,
            1,
            MAX_SOCKET_TIMEOUT.as_millis() as u64,
        )?;
        validate_u64(
            "max_result_rows",
            max_result_rows,
            1,
            MAX_CONFIGURED_RESULT_ROWS as u64,
        )?;
        Self::new(
            max_connections as usize,
            Duration::from_millis(idle_timeout_ms),
            Duration::from_millis(write_timeout_ms),
            max_result_rows as usize,
        )
    }

    /// Returns the maximum number of concurrently admitted connections.
    #[must_use]
    pub const fn max_connections(self) -> usize {
        self.max_connections
    }

    /// Returns the socket read timeout for idle or stalled clients.
    #[must_use]
    pub const fn idle_timeout(self) -> Duration {
        self.idle_timeout
    }

    /// Returns the socket write timeout for protocol responses.
    #[must_use]
    pub const fn write_timeout(self) -> Duration {
        self.write_timeout
    }

    /// Returns the maximum rows allowed in one materialized query result.
    #[must_use]
    pub const fn max_result_rows(self) -> usize {
        self.session_policy.max_result_rows()
    }

    /// Returns the policy installed into each newly admitted session.
    #[must_use]
    pub const fn session_policy(self) -> SessionPolicy {
        self.session_policy
    }
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            session_policy: SessionPolicy::default(),
        }
    }
}

/// A manifest or API server-limit value outside its supported range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLimitsError {
    field: &'static str,
    value: u128,
    minimum: u128,
    maximum: u128,
}

impl ServerLimitsError {
    fn new(field: &'static str, value: u128, minimum: u128, maximum: u128) -> Self {
        Self {
            field,
            value,
            minimum,
            maximum,
        }
    }

    /// Returns the manifest field name whose value was invalid.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ServerLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "server limit `{}` value {} is outside {}..={}",
            self.field, self.value, self.minimum, self.maximum
        )
    }
}

impl Error for ServerLimitsError {}

fn validate_usize(
    field: &'static str,
    value: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), ServerLimitsError> {
    if (minimum..=maximum).contains(&value) {
        Ok(())
    } else {
        Err(ServerLimitsError::new(
            field,
            value as u128,
            minimum as u128,
            maximum as u128,
        ))
    }
}

fn validate_u64(
    field: &'static str,
    value: u64,
    minimum: u64,
    maximum: u64,
) -> Result<(), ServerLimitsError> {
    if (minimum..=maximum).contains(&value) {
        Ok(())
    } else {
        Err(ServerLimitsError::new(
            field,
            u128::from(value),
            u128::from(minimum),
            u128::from(maximum),
        ))
    }
}

fn validate_timeout(field: &'static str, value: Duration) -> Result<(), ServerLimitsError> {
    if (MIN_SOCKET_TIMEOUT..=MAX_SOCKET_TIMEOUT).contains(&value) {
        Ok(())
    } else {
        Err(ServerLimitsError::new(
            field,
            value.as_millis(),
            MIN_SOCKET_TIMEOUT.as_millis(),
            MAX_SOCKET_TIMEOUT.as_millis(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_shared_with_the_session_policy() {
        let limits = ServerLimits::default();
        assert_eq!(limits.max_connections(), DEFAULT_MAX_CONNECTIONS);
        assert_eq!(limits.idle_timeout(), DEFAULT_IDLE_TIMEOUT);
        assert_eq!(limits.write_timeout(), DEFAULT_WRITE_TIMEOUT);
        assert_eq!(limits.max_result_rows(), DEFAULT_MAX_RESULT_ROWS);
        assert_eq!(
            limits.session_policy().max_result_rows(),
            SessionPolicy::default().max_result_rows()
        );
    }

    #[test]
    fn every_limit_rejects_zero_and_values_above_its_bound() {
        assert!(ServerLimits::from_millis(0, 1, 1, 1).is_err());
        assert!(ServerLimits::from_millis(MAX_CONFIGURED_CONNECTIONS as u64 + 1, 1, 1, 1).is_err());
        assert!(ServerLimits::from_millis(1, 0, 1, 1).is_err());
        assert!(
            ServerLimits::from_millis(1, MAX_SOCKET_TIMEOUT.as_millis() as u64 + 1, 1, 1,).is_err()
        );
        assert!(ServerLimits::from_millis(1, 1, 0, 1).is_err());
        assert!(ServerLimits::from_millis(1, 1, 1, 0).is_err());
        assert!(
            ServerLimits::from_millis(1, 1, 1, MAX_CONFIGURED_RESULT_ROWS as u64 + 1,).is_err()
        );
    }
}
