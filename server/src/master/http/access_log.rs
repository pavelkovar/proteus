//! Wraps a response body so its access-log line is emitted exactly once,
//! when the body finishes or is dropped.

use super::connection::ConnBusyGuard;
use super::proxy::ClientIdentity;
use super::{InFlightGuard, ResponseBody};
use crate::logging;
use bytes::Bytes;
use http_body::Frame;
use hyper::{Method, StatusCode};
use std::sync::Arc;
use std::time::Instant;

/// Captured while the request is in scope, emitted once the body finishes.
/// Holds the raw `Uri` since a log records what the client actually sent.
pub(super) struct PendingAccessLog {
    pub(super) client_ip: ClientIdentity,
    pub(super) method: Method,
    pub(super) uri: hyper::Uri,
    pub(super) status: StatusCode,
    pub(super) start: Instant,
    pub(super) action: &'static str,
    pub(super) worker_pid: u32,
    pub(super) php_target: Option<Arc<str>>,
}

impl PendingAccessLog {
    /// 0 for anything that never reached a PHP worker; a real pid is never 0.
    fn emit(&self, bytes_sent: u64) {
        logging::info!(
            r#type = "access_log",
            client_ip = %self.client_ip,
            method = %self.method,
            path = self.uri.path_and_query().map_or("", |pq| pq.as_str()),
            status = self.status.as_u16(),
            duration_ms = self.start.elapsed().as_millis() as u64,
            worker_pid = self.worker_pid,
            action = self.action,
            php_target = self.php_target.as_deref().unwrap_or(""),
            body_bytes_sent = bytes_sent,
        );
    }
}

/// Holds the in-flight guard until the body is fully streamed, so shutdown
/// can't truncate a live response. Also owns the access-log line, emitted
/// from `Drop` so a stream dying halfway is still logged.
pub(super) struct GuardedBody {
    pub(super) inner: ResponseBody,
    pub(super) _guard: InFlightGuard,
    pub(super) _conn: ConnBusyGuard,
    pub(super) log: Option<PendingAccessLog>,
    pub(super) bytes_sent: u64,
}

impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        if let std::task::Poll::Ready(Some(Ok(frame))) = &polled
            && let Some(data) = frame.data_ref()
        {
            self.bytes_sent += data.len() as u64;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for GuardedBody {
    fn drop(&mut self) {
        if let Some(log) = self.log.take() {
            log.emit(self.bytes_sent);
        }
    }
}
