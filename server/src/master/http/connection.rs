//! Per-connection state: busy/idle tracking and the timeouts read once per
//! accept.

use super::AppState;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

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
    pub(super) write_stall: Option<std::time::Duration>,
}

impl ConnTimeouts {
    pub(super) fn from(state: &AppState) -> Self {
        ConnTimeouts {
            header_read: std::time::Duration::from_secs(
                state.config.connection.header_read_timeout,
            ),
            idle: std::time::Duration::from_secs(state.config.connection.idle_timeout),
            write_stall: (state.config.connection.body_write_timeout > 0).then(|| {
                std::time::Duration::from_secs(state.config.connection.body_write_timeout)
            }),
        }
    }
}

/// Fails a write the client has not acknowledged anything of for `timeout`, so
/// hyper drops the connection (cut off, never ended as complete) and the PHP
/// worker blocked behind the body channel is released.
pub(super) struct WriteStall<T> {
    inner: T,
    timeout: Option<std::time::Duration>,
    deadline: Option<Pin<Box<Sleep>>>,
    stalled: bool,
    acked: Option<u64>,
}

/// Progress for a reader too slow to make the socket writable again (Linux
/// wants half the send buffer free). `None` before 4.1 or on non-TCP.
fn bytes_acked(fd: std::os::fd::RawFd) -> Option<u64> {
    let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = size_of::<libc::tcp_info>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            (&raw mut info).cast(),
            &mut len,
        )
    };
    let needed = std::mem::offset_of!(libc::tcp_info, tcpi_bytes_acked) + size_of::<u64>();
    (rc == 0 && len as usize >= needed).then_some(info.tcpi_bytes_acked)
}

impl<T: std::os::fd::AsRawFd> WriteStall<T> {
    pub(super) fn new(inner: T, timeout: Option<std::time::Duration>) -> Self {
        WriteStall {
            inner,
            timeout,
            deadline: None,
            stalled: false,
            acked: None,
        }
    }

    fn check<R>(
        &mut self,
        cx: &mut Context<'_>,
        poll: Poll<std::io::Result<R>>,
    ) -> Poll<std::io::Result<R>> {
        let Some(timeout) = self.timeout else {
            return poll;
        };
        if poll.is_ready() {
            self.stalled = false;
            return poll;
        }
        let fd = self.inner.as_raw_fd();
        let at = tokio::time::Instant::now() + timeout;
        let deadline = self
            .deadline
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(at)));
        if !self.stalled {
            self.stalled = true;
            self.acked = bytes_acked(fd);
            deadline.as_mut().reset(at);
        }
        loop {
            ready!(deadline.as_mut().poll(cx));
            let acked = bytes_acked(fd);
            if acked.is_none() || acked <= self.acked {
                break;
            }
            self.acked = acked;
            deadline
                .as_mut()
                .reset(tokio::time::Instant::now() + timeout);
        }
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "client took no response bytes within body_write_timeout",
        )))
    }
}

impl<T: AsyncRead + std::os::fd::AsRawFd + Unpin> AsyncRead for WriteStall<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + std::os::fd::AsRawFd + Unpin> AsyncWrite for WriteStall<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.check(cx, poll)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.check(cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_flush(cx);
        self.check(cx, poll)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
