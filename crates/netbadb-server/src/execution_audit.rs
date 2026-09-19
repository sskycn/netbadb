#[cfg(feature = "execution-audit")]
use std::sync::Arc;
#[cfg(feature = "execution-audit")]
use std::sync::Mutex;
#[cfg(feature = "execution-audit")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "execution-audit")]
use std::time::{Duration, Instant};

/// Benchmark-only aggregate timings for one server instance.
#[cfg(feature = "execution-audit")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerExecutionAuditSnapshot {
    pub submitted_requests: u64,
    pub queue_depth: u64,
    pub max_queue_depth: u64,
    pub completed_requests: u64,
    pub worker_active: u64,
    pub queue_wait_ns: u64,
    pub max_queue_wait_ns: u64,
    pub worker_busy_ns: u64,
    pub worker_idle_ns: u64,
    pub socket_write_ns: u64,
    pub total_request_ns: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ServerExecutionAudit {
    #[cfg(feature = "execution-audit")]
    inner: Arc<ServerExecutionAuditInner>,
}

#[cfg(feature = "execution-audit")]
#[derive(Debug)]
struct ServerExecutionAuditInner {
    submitted_requests: AtomicU64,
    queue_depth: AtomicU64,
    max_queue_depth: AtomicU64,
    completed_requests: AtomicU64,
    worker_active: AtomicU64,
    queue_wait_ns: AtomicU64,
    max_queue_wait_ns: AtomicU64,
    worker_busy_ns: AtomicU64,
    worker_idle_ns: AtomicU64,
    last_worker_completion: Mutex<Instant>,
    socket_write_ns: AtomicU64,
    total_request_ns: AtomicU64,
}

#[cfg(feature = "execution-audit")]
impl Default for ServerExecutionAuditInner {
    fn default() -> Self {
        Self {
            submitted_requests: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            completed_requests: AtomicU64::new(0),
            worker_active: AtomicU64::new(0),
            queue_wait_ns: AtomicU64::new(0),
            max_queue_wait_ns: AtomicU64::new(0),
            worker_busy_ns: AtomicU64::new(0),
            worker_idle_ns: AtomicU64::new(0),
            last_worker_completion: Mutex::new(Instant::now()),
            socket_write_ns: AtomicU64::new(0),
            total_request_ns: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestAuditTicket {
    #[cfg(feature = "execution-audit")]
    submitted_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerAuditTicket {
    #[cfg(feature = "execution-audit")]
    started_at: Instant,
}

impl ServerExecutionAudit {
    pub(crate) fn submitted(&self) -> RequestAuditTicket {
        #[cfg(feature = "execution-audit")]
        {
            saturating_increment(&self.inner.submitted_requests);
            let depth = saturating_increment(&self.inner.queue_depth);
            update_max(&self.inner.max_queue_depth, depth);
            RequestAuditTicket {
                submitted_at: Instant::now(),
            }
        }
        #[cfg(not(feature = "execution-audit"))]
        RequestAuditTicket {}
    }

    pub(crate) fn worker_started(&self, ticket: RequestAuditTicket) -> WorkerAuditTicket {
        #[cfg(feature = "execution-audit")]
        {
            let started_at = Instant::now();
            self.inner.worker_active.store(1, Ordering::Relaxed);
            if let Ok(last_completion) = self.inner.last_worker_completion.lock() {
                saturating_add(
                    &self.inner.worker_idle_ns,
                    nanos(started_at.duration_since(*last_completion)),
                );
            }
            saturating_decrement(&self.inner.queue_depth);
            let queue_wait = nanos(started_at.duration_since(ticket.submitted_at));
            saturating_add(&self.inner.queue_wait_ns, queue_wait);
            update_max(&self.inner.max_queue_wait_ns, queue_wait);
            WorkerAuditTicket { started_at }
        }
        #[cfg(not(feature = "execution-audit"))]
        {
            let _ = ticket;
            WorkerAuditTicket {}
        }
    }

    pub(crate) fn submission_failed(&self, ticket: RequestAuditTicket) {
        #[cfg(feature = "execution-audit")]
        {
            let _ = ticket;
            saturating_decrement(&self.inner.queue_depth);
        }
        #[cfg(not(feature = "execution-audit"))]
        let _ = ticket;
    }

    pub(crate) fn worker_completed(&self, ticket: WorkerAuditTicket) {
        #[cfg(feature = "execution-audit")]
        {
            saturating_add(
                &self.inner.worker_busy_ns,
                nanos(ticket.started_at.elapsed()),
            );
            saturating_increment(&self.inner.completed_requests);
            self.inner.worker_active.store(0, Ordering::Relaxed);
            if let Ok(mut last_completion) = self.inner.last_worker_completion.lock() {
                *last_completion = Instant::now();
            }
        }
        #[cfg(not(feature = "execution-audit"))]
        let _ = ticket;
    }

    #[cfg(feature = "execution-audit")]
    pub(crate) fn socket_write(&self, elapsed: std::time::Duration) {
        saturating_add(&self.inner.socket_write_ns, nanos(elapsed));
    }

    #[cfg(feature = "execution-audit")]
    pub(crate) fn total_request(&self, elapsed: std::time::Duration) {
        saturating_add(&self.inner.total_request_ns, nanos(elapsed));
    }

    #[cfg(feature = "execution-audit")]
    pub(crate) fn snapshot(&self) -> ServerExecutionAuditSnapshot {
        ServerExecutionAuditSnapshot {
            submitted_requests: self.inner.submitted_requests.load(Ordering::Relaxed),
            queue_depth: self.inner.queue_depth.load(Ordering::Relaxed),
            max_queue_depth: self.inner.max_queue_depth.load(Ordering::Relaxed),
            completed_requests: self.inner.completed_requests.load(Ordering::Relaxed),
            worker_active: self.inner.worker_active.load(Ordering::Relaxed),
            queue_wait_ns: self.inner.queue_wait_ns.load(Ordering::Relaxed),
            max_queue_wait_ns: self.inner.max_queue_wait_ns.load(Ordering::Relaxed),
            worker_busy_ns: self.inner.worker_busy_ns.load(Ordering::Relaxed),
            worker_idle_ns: self.inner.worker_idle_ns.load(Ordering::Relaxed),
            socket_write_ns: self.inner.socket_write_ns.load(Ordering::Relaxed),
            total_request_ns: self.inner.total_request_ns.load(Ordering::Relaxed),
        }
    }

    #[cfg(feature = "execution-audit")]
    pub(crate) fn reset(&self) {
        self.inner.submitted_requests.store(0, Ordering::Relaxed);
        self.inner.queue_depth.store(0, Ordering::Relaxed);
        self.inner.max_queue_depth.store(0, Ordering::Relaxed);
        self.inner.completed_requests.store(0, Ordering::Relaxed);
        self.inner.queue_wait_ns.store(0, Ordering::Relaxed);
        self.inner.max_queue_wait_ns.store(0, Ordering::Relaxed);
        self.inner.worker_busy_ns.store(0, Ordering::Relaxed);
        self.inner.worker_idle_ns.store(0, Ordering::Relaxed);
        self.inner.socket_write_ns.store(0, Ordering::Relaxed);
        self.inner.total_request_ns.store(0, Ordering::Relaxed);
        if let Ok(mut last_completion) = self.inner.last_worker_completion.lock() {
            *last_completion = Instant::now();
        }
    }
}

#[cfg(feature = "execution-audit")]
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(feature = "execution-audit")]
fn saturating_increment(counter: &AtomicU64) -> u64 {
    let previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        })
        .unwrap_or_else(|value| value);
    previous.saturating_add(1)
}

#[cfg(feature = "execution-audit")]
fn saturating_decrement(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

#[cfg(feature = "execution-audit")]
fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

#[cfg(feature = "execution-audit")]
fn update_max(counter: &AtomicU64, candidate: u64) {
    let _ = counter.fetch_max(candidate, Ordering::Relaxed);
}
