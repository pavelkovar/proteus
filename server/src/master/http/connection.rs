//! Per-connection state: busy/idle tracking and the timeouts read once per
//! accept.

use super::AppState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// Lets an idle keep-alive connection be closed without disturbing one
/// merely waiting on a slow PHP request: wall-clock silence can't tell them
/// apart, so idle means no request in flight, not no recent traffic.
pub(super) struct ConnState {
    in_flight: AtomicU64,
    /// Starts at accept time, so a client that connects and sends nothing is
    /// idle from the outset.
    last_finished_ms: AtomicU64,
    epoch: Instant,
}

impl ConnState {
    pub(super) fn new() -> Self {
        ConnState {
            in_flight: AtomicU64::new(0),
            last_finished_ms: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    fn request_started(&self) {
        self.in_flight.fetch_add(1, Relaxed);
    }

    /// Not a `Gauge`, because the order here is load-bearing: decrementing
    /// first would let `idle_for` pair a zero count with the *previous*
    /// request's stamp and close a connection that just went idle.
    fn request_finished(&self) {
        self.last_finished_ms
            .store(self.epoch.elapsed().as_millis() as u64, Relaxed);
        self.in_flight.fetch_sub(1, Relaxed);
    }

    fn idle_for(&self) -> Option<std::time::Duration> {
        if self.in_flight.load(Relaxed) > 0 {
            return None;
        }
        let since = self.epoch.elapsed().as_millis() as u64 - self.last_finished_ms.load(Relaxed);
        Some(std::time::Duration::from_millis(since))
    }
}

/// Marks the connection busy for the life of the request, response body
/// included. Protects keep-alive reuse: without it the connection could
/// close behind a request slower than `idle_timeout`.
pub(super) struct ConnBusyGuard(Arc<ConnState>);

impl ConnBusyGuard {
    pub(super) fn new(conn: Arc<ConnState>) -> Self {
        conn.request_started();
        ConnBusyGuard(conn)
    }
}

impl Drop for ConnBusyGuard {
    fn drop(&mut self) {
        self.0.request_finished();
    }
}

/// Closes the connection once it has been idle past `idle_timeout`. Sleeps
/// to the exact moment the deadline could first be reached, rather than
/// polling and costing every open connection a wakeup per interval forever.
pub(super) async fn wait_until_idle(conn: &ConnState, idle_timeout: std::time::Duration) {
    loop {
        match conn.idle_for() {
            // In flight, so the earliest it can go idle is a full timeout away.
            None => tokio::time::sleep(idle_timeout).await,
            Some(idle) if idle >= idle_timeout => return,
            // Quiet but not long enough; sleep out the exact remainder.
            Some(idle) => tokio::time::sleep(idle_timeout - idle).await,
        }
    }
}

/// Read once per accept rather than per request.
#[derive(Clone, Copy)]
pub(super) struct ConnTimeouts {
    pub(super) header_read: std::time::Duration,
    pub(super) idle: std::time::Duration,
}

impl ConnTimeouts {
    pub(super) fn from(state: &AppState) -> Self {
        ConnTimeouts {
            header_read: std::time::Duration::from_secs(
                state.config.connection.header_read_timeout,
            ),
            idle: std::time::Duration::from_secs(state.config.connection.idle_timeout),
        }
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
