use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time operational counters for one running server.
///
/// Fields are loaded independently with relaxed ordering, so a snapshot is
/// suitable for observation but is not a transactional view of concurrent
/// connection activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerMetricsSnapshot {
    pub accepted_connections_total: u64,
    pub rejected_connections_total: u64,
    pub closed_connections_total: u64,
    pub active_connections: u64,
    pub protocol_failures_total: u64,
    pub worker_requests_total: u64,
    pub query_response_limit_errors_total: u64,
    pub idle_timeouts_total: u64,
    pub write_failures_total: u64,
    pub tls_handshakes_total: u64,
    pub tls_handshake_failures_total: u64,
    pub authenticated_connections_total: u64,
    pub authorization_denials_total: u64,
}

/// Cloneable, read-only access to a running server's operational metrics.
#[derive(Debug, Clone)]
pub struct ServerMetricsHandle {
    inner: Arc<ServerMetrics>,
}

impl ServerMetricsHandle {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ServerMetrics::default()),
        }
    }

    /// Captures the current metric values.
    #[must_use]
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        self.inner.snapshot()
    }

    pub(crate) fn accepted(&self) {
        saturating_increment(&self.inner.accepted_connections_total);
        saturating_increment(&self.inner.active_connections);
    }

    pub(crate) fn rejected(&self) {
        saturating_increment(&self.inner.rejected_connections_total);
    }

    pub(crate) fn closed(&self) {
        saturating_increment(&self.inner.closed_connections_total);
        saturating_decrement(&self.inner.active_connections);
    }

    pub(crate) fn protocol_failure(&self) {
        saturating_increment(&self.inner.protocol_failures_total);
    }

    pub(crate) fn worker_request(&self) {
        saturating_increment(&self.inner.worker_requests_total);
    }

    pub(crate) fn query_response_limit_error(&self) {
        saturating_increment(&self.inner.query_response_limit_errors_total);
    }

    pub(crate) fn idle_timeout(&self) {
        saturating_increment(&self.inner.idle_timeouts_total);
    }

    pub(crate) fn write_failure(&self) {
        saturating_increment(&self.inner.write_failures_total);
    }

    pub(crate) fn tls_handshake(&self) {
        saturating_increment(&self.inner.tls_handshakes_total);
    }

    pub(crate) fn tls_handshake_failure(&self) {
        saturating_increment(&self.inner.tls_handshake_failures_total);
    }

    pub(crate) fn authenticated_connection(&self) {
        saturating_increment(&self.inner.authenticated_connections_total);
    }

    pub(crate) fn authorization_denial(&self) {
        saturating_increment(&self.inner.authorization_denials_total);
    }
}

#[derive(Debug, Default)]
struct ServerMetrics {
    accepted_connections_total: AtomicU64,
    rejected_connections_total: AtomicU64,
    closed_connections_total: AtomicU64,
    active_connections: AtomicU64,
    protocol_failures_total: AtomicU64,
    worker_requests_total: AtomicU64,
    query_response_limit_errors_total: AtomicU64,
    idle_timeouts_total: AtomicU64,
    write_failures_total: AtomicU64,
    tls_handshakes_total: AtomicU64,
    tls_handshake_failures_total: AtomicU64,
    authenticated_connections_total: AtomicU64,
    authorization_denials_total: AtomicU64,
}

impl ServerMetrics {
    fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            accepted_connections_total: self.accepted_connections_total.load(Ordering::Relaxed),
            rejected_connections_total: self.rejected_connections_total.load(Ordering::Relaxed),
            closed_connections_total: self.closed_connections_total.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            protocol_failures_total: self.protocol_failures_total.load(Ordering::Relaxed),
            worker_requests_total: self.worker_requests_total.load(Ordering::Relaxed),
            query_response_limit_errors_total: self
                .query_response_limit_errors_total
                .load(Ordering::Relaxed),
            idle_timeouts_total: self.idle_timeouts_total.load(Ordering::Relaxed),
            write_failures_total: self.write_failures_total.load(Ordering::Relaxed),
            tls_handshakes_total: self.tls_handshakes_total.load(Ordering::Relaxed),
            tls_handshake_failures_total: self.tls_handshake_failures_total.load(Ordering::Relaxed),
            authenticated_connections_total: self
                .authenticated_connections_total
                .load(Ordering::Relaxed),
            authorization_denials_total: self.authorization_denials_total.load(Ordering::Relaxed),
        }
    }
}

fn saturating_increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    });
}

fn saturating_decrement(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_track_lifecycle_without_underflowing_active_connections() {
        let metrics = ServerMetricsHandle::new();
        metrics.closed();
        metrics.accepted();
        metrics.worker_request();
        metrics.protocol_failure();
        metrics.query_response_limit_error();
        metrics.idle_timeout();
        metrics.write_failure();
        metrics.tls_handshake();
        metrics.tls_handshake_failure();
        metrics.authenticated_connection();
        metrics.authorization_denial();
        metrics.rejected();
        metrics.closed();
        assert_eq!(
            metrics.snapshot(),
            ServerMetricsSnapshot {
                accepted_connections_total: 1,
                rejected_connections_total: 1,
                closed_connections_total: 2,
                active_connections: 0,
                protocol_failures_total: 1,
                worker_requests_total: 1,
                query_response_limit_errors_total: 1,
                idle_timeouts_total: 1,
                write_failures_total: 1,
                tls_handshakes_total: 1,
                tls_handshake_failures_total: 1,
                authenticated_connections_total: 1,
                authorization_denials_total: 1,
            }
        );
    }
}
