//! HTTP entry point: connection accept and drain, request handling, client
//! IP resolution, access log.

mod compression;
mod conditional;
mod fs_cache;
mod php_dispatch;
mod proxy;
mod range;
mod response;
mod routing;

use conditional::*;
pub(crate) use fs_cache::FsCache;
use proxy::*;
use response::*;
use routing::*;

use crate::config::{Config, RouteActionConfig};
use crate::master::pool_manager::PoolManager;
use bytes::Bytes;
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

pub struct AppState {
    /// `Arc` so a background finish-watch task can hold its own reference.
    pub pool: Arc<PoolManager>,
    pub config: Config,
    /// Polled after SIGTERM to know when it is safe to stop.
    pub in_flight: AtomicU64,
    /// Existence and type only, never content.
    pub fs_cache: FsCache,
    /// Decided once at startup, so a request need not scan every route just
    /// to learn whether it has to resolve a Host at all.
    pub uses_host_matching: bool,
}

impl AppState {
    pub fn new(pool: Arc<PoolManager>, config: Config, fs_cache: FsCache) -> Self {
        let uses_host_matching = config.routes.iter().any(|r| !r.matcher.host.is_empty());
        AppState { pool, config, in_flight: AtomicU64::new(0), fs_cache, uses_host_matching }
    }
}

/// RAII so every early return decrements, panics included. Owns its state
/// rather than borrowing, since it moves into a body that outlives `handle`.
struct InFlightGuard(Arc<AppState>);

impl InFlightGuard {
    fn new(state: Arc<AppState>) -> Self {
        state.in_flight.fetch_add(1, Relaxed);
        InFlightGuard(state)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Relaxed);
    }
}

/// Marks the connection busy for the life of the request, response body
/// included.
///
/// What this protects is keep-alive reuse, not the request: the idle path
/// shuts down gracefully, so a request slower than `idle_timeout` completes
/// either way - but without this the connection closes behind it and a
/// client with more to send has to reconnect.
struct ConnBusyGuard(Arc<ConnState>);

impl ConnBusyGuard {
    fn new(conn: Arc<ConnState>) -> Self {
        conn.request_started();
        ConnBusyGuard(conn)
    }
}

impl Drop for ConnBusyGuard {
    fn drop(&mut self) {
        self.0.request_finished();
    }
}


type ResponseBody = BoxBody<Bytes, std::io::Error>;

/// How a response body ended, for the access log.
#[derive(Clone, Copy)]
enum BodyOutcome {
    Complete,

    Failed,
    /// Dropped before the last frame: the client went away.
    Aborted,
}

impl BodyOutcome {
    fn as_str(self) -> &'static str {
        match self {
            BodyOutcome::Complete => "complete",
            BodyOutcome::Failed => "error",
            BodyOutcome::Aborted => "aborted",
        }
    }
}

/// Captured while the request is in scope, emitted once the body finishes.
///
/// Holds the raw `Uri`, both because a log records what the client actually
/// sent and because it is refcounted and already cloned.
struct PendingAccessLog {
    client_ip: IpAddr,
    method: Method,
    uri: hyper::Uri,
    status: StatusCode,
    start: Instant,
    action: &'static str,
    worker_pid: u32,
    php_target: String,
}

impl PendingAccessLog {
    /// 0 for anything that never reached a PHP worker; a real pid is never 0.
    fn emit(&self, body: BodyOutcome) {
        tracing::info!(
            r#type = "access_log",
            client_ip = %self.client_ip,
            method = %self.method,
            path = self.uri.path(),
            status = self.status.as_u16(),
            duration_ms = self.start.elapsed().as_millis() as u64,
            worker_pid = self.worker_pid,
            action = self.action,
            php_target = %self.php_target,
            body = body.as_str(),
        );
    }
}

/// Holds the in-flight guard until the body is fully streamed, without which
/// the shutdown drain could exit and truncate a live response.
///
/// It also owns the access-log line, emitted from `Drop` so that
/// `duration_ms` covers the body transfer and a stream that dies halfway is
/// not recorded as a clean 200. No explicit call site could catch the client
/// hanging up mid-body.
struct GuardedBody {
    inner: ResponseBody,
    _guard: InFlightGuard,
    _conn: ConnBusyGuard,
    log: Option<PendingAccessLog>,
    outcome: BodyOutcome,
}

impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        match &polled {
            std::task::Poll::Ready(None) => self.outcome = BodyOutcome::Complete,
            std::task::Poll::Ready(Some(Err(_))) => self.outcome = BodyOutcome::Failed,
            _ => {}
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
            log.emit(self.outcome);
        }
    }
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
    peer_ip: IpAddr,
    listen_addr: &str,
    conn: Arc<ConnState>,
) -> Result<Response<ResponseBody>, std::convert::Infallible> {
    let in_flight = InFlightGuard::new(state.clone());
    let conn_busy = ConnBusyGuard::new(conn);
    let start = Instant::now();
    let method = req.method().clone();
    // A refcount bump, not a copy. Needed because `req` is moved below, so
    // the path cannot simply borrow from it.
    let uri = req.uri().clone();
    // Decoded once, for routing, the filesystem and PATH_INFO alike.
    // REQUEST_URI keeps the raw form, as every other SAPI reports it.
    let decoded_path = match percent_decode_path(uri.path()) {
        Ok(path) => path,
        Err(reason) => {
            let resp = build_response(
                StatusCode::BAD_REQUEST,
                b"400 invalid path\n".to_vec(),
                &crate::ipc::data::HeaderBlob::default(),
            );
            tracing::debug!(r#type = "controller", ?reason, raw_path = uri.path(), "rejected an undecodable request path");
            let log = PendingAccessLog {
                client_ip: peer_ip,
                method,
                uri,
                status: resp.status(),
                start,
                action: "rejected",
                worker_pid: 0,
                php_target: String::new(),
            };
            return Ok(resp.map(|inner| {
                GuardedBody {
                    inner,
                    _guard: in_flight,
                    _conn: conn_busy,
                    log: Some(log),
                    outcome: BodyOutcome::Complete,
                }
                .boxed()
            }));
        }
    };
    let path = decoded_path.as_ref();
    let is_trusted_peer = ip_is_trusted_proxy(peer_ip, &state.config.trusted_proxies);
    let client_ip = resolve_client_ip(peer_ip, is_trusted_peer, req.headers(), |ip| {
        ip_is_trusted_proxy(ip, &state.config.trusted_proxies)
    });
    // Shares the buffer rather than copying the string out.
    let accept_encoding = req.headers().get(hyper::header::ACCEPT_ENCODING).cloned();
    let accept_encoding = accept_encoding.as_ref().and_then(|v| v.to_str().ok()).unwrap_or("");
    let min_size = state.config.compression.min_size_bytes;

    // Skipped unless some route matches on it.
    let host_for_matching = if state.uses_host_matching {
        resolve_server_name_port(req.headers(), is_trusted_peer, listen_addr).0.to_ascii_lowercase()
    } else {
        String::new()
    };
    let decision = match_route(&state.config, path, method.as_str(), &host_for_matching);
    // Only Static consults these.
    let is_static_route = matches!(&decision, RouteDecision::Matched { action: RouteActionConfig::Static { .. }, .. });
    let conditional = if is_static_route { ConditionalHeaders::from_headers(req.headers()) } else { ConditionalHeaders::default() };

    let mime_types = &state.config.compression.mime_types;

    // Once, before any routing, rather than per match arm: a new arm added
    // without repeating the check would silently admit traversal.
    if path_escapes_root(path) {
        let resp = build_response(StatusCode::BAD_REQUEST, b"400 invalid path\n".to_vec(), &crate::ipc::data::HeaderBlob::default());
        let log = PendingAccessLog {
            client_ip,
            method,
            uri,
            status: resp.status(),
            start,
            action: "rejected",
            worker_pid: 0,
            php_target: String::new(),
        };
        return Ok(resp.map(|inner| {
            GuardedBody {
                inner,
                _guard: in_flight,
                _conn: conn_busy,
                log: Some(log),
                outcome: BodyOutcome::Complete,
            }
            .boxed()
        }));
    }

    let ctx = RequestContext { client_ip, listen_addr, is_trusted_peer };
    let DispatchResult { action_body, log_action, worker_pid, php_target } = match decision {
        RouteDecision::Matched { action } => dispatch_action(&state, action, req, path, ctx).await,
        RouteDecision::NoMatch => DispatchResult::new(ActionBody::not_found(), "none", 0),
    };

    let resp = match action_body {
        ActionBody::StaticFile { file, meta, candidate } => {
            build_static_response(
                file,
                &meta,
                &candidate,
                path,
                accept_encoding,
                min_size,
                mime_types,
                &conditional,
                method == Method::HEAD,
            )
            .await
        }
        ActionBody::PhpStream { status, headers, body } => {
            build_php_stream_response(status, &headers, body, accept_encoding, min_size, mime_types)
        }
        ActionBody::Buffered { status, body, headers } => build_response(status, body, &headers),
    };
    let log = PendingAccessLog {
        client_ip,
        method,
        uri,
        status: resp.status(),
        start,
        action: log_action,
        worker_pid,
        php_target,
    };
    // `Aborted` until proven otherwise, so a body dropped before its end
    // records the client hanging up.
    Ok(resp.map(|inner| {
        GuardedBody { inner, _guard: in_flight, _conn: conn_busy, log: Some(log), outcome: BodyOutcome::Aborted }.boxed()
    }))
}

/// Lets an idle keep-alive connection be closed without disturbing one
/// merely waiting on a slow PHP request.
///
/// Wall-clock silence cannot tell those apart, since a socket sees no
/// traffic for as long as a script runs. Idle is therefore defined as no
/// request in flight plus time since the last one finished.
struct ConnState {
    in_flight: AtomicU64,
    /// Starts at accept time, so a client that connects and sends nothing is
    /// idle from the outset.
    last_finished_ms: AtomicU64,
    epoch: Instant,
}

impl ConnState {
    fn new() -> Self {
        ConnState { in_flight: AtomicU64::new(0), last_finished_ms: AtomicU64::new(0), epoch: Instant::now() }
    }

    fn request_started(&self) {
        self.in_flight.fetch_add(1, Relaxed);
    }

    fn request_finished(&self) {
        self.last_finished_ms.store(self.epoch.elapsed().as_millis() as u64, Relaxed);
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

/// Closes the connection once it has been idle past `idle_timeout`.
///
/// Sleeps to the exact moment the deadline could first be reached rather
/// than ticking towards it, so a quiet connection wakes once and a busy one
/// wakes about once per request. Polling instead would cost every open
/// connection a wakeup per interval forever, just to observe no change.
async fn wait_until_idle(conn: &ConnState, idle_timeout: std::time::Duration) {
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

/// Traffic here is small and latency-sensitive, never the bulk transfer
/// Nagle helps. Left on, Nagle plus the client's delayed ACK is the classic
/// fixed-40ms-per-request bug.
fn set_nodelay_or_log(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::warn!(r#type = "controller", error = %e, "set_nodelay failed");
    }
}

/// Deliberately a separate listener from public traffic.
async fn serve_status(listen: String, state: Arc<AppState>) {
    let header_read_timeout = std::time::Duration::from_secs(state.config.connection.header_read_timeout);
    let listener = TcpListener::bind(&listen).await.expect("status bind failed");
    tracing::info!(r#type = "controller", %listen, "status endpoint listening");
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(r#type = "controller", error = %e, "status accept failed");
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            set_nodelay_or_log(&stream);
            let io = TokioIo::new(stream);
            let service = hyper::service::service_fn(move |_req: Request<Incoming>| {
                let state = state.clone();
                async move {
                    let body = state.pool.status_json().to_string();
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .header(hyper::header::CONTENT_TYPE, "application/json")
                            .body(Full::new(Bytes::from(body)))
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(header_read_timeout)
                .serve_connection(io, service)
                .await;
        });
    }
}

/// SIGTERM (systemd/docker/k8s graceful stop) or SIGINT (Ctrl-C) - same
/// drain either way.
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!(r#type = "controller", "received SIGTERM"),
        _ = sigint.recv() => tracing::info!(r#type = "controller", "received SIGINT"),
    }
}

pub async fn serve(state: Arc<AppState>) {
    let status_listen = state.config.status.listen.clone();
    tokio::spawn(serve_status(status_listen, state.clone()));

    // Raced against accept() so shutdown stops new connections; in-flight
    // requests and existing keep-alives finish on their own.
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Zero means no cap, expressed as a huge permit count so the acquire
    // path stays branchless.
    let conn_cap = state.config.connection.max;
    let connection_slots =
        Arc::new(tokio::sync::Semaphore::new(if conn_cap == 0 { tokio::sync::Semaphore::MAX_PERMITS } else { conn_cap }));
    let header_read_timeout = std::time::Duration::from_secs(state.config.connection.header_read_timeout);
    let idle_timeout = std::time::Duration::from_secs(state.config.connection.idle_timeout);

    for listen in &state.config.listen {
        let listener = TcpListener::bind(listen).await.expect("bind failed");
        tracing::info!(r#type = "controller", %listen, "listening");
        let state = state.clone();
        // The closure needs an owned copy per call, and a refcount bump
        // beats a heap allocation per request on a keep-alive connection.
        let listen_addr: Arc<str> = Arc::from(listen.as_str());
        let shutdown = shutdown.clone();
        let connection_slots = Arc::clone(&connection_slots);
        tokio::spawn(async move {
            loop {
                let (stream, peer) = tokio::select! {
                    result = listener.accept() => match result {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(r#type = "controller", error = %e, "accept failed");
                            continue;
                        }
                    },
                    _ = shutdown.notified() => {
                        tracing::info!(r#type = "controller", %listen_addr, "no longer accepting new connections");
                        break;
                    }
                };
                // Before spawning, so a flood waits in the kernel's bounded
                // accept backlog rather than becoming an unbounded pile of
                // tasks each holding a socket and its buffers.
                let Ok(slot) = Arc::clone(&connection_slots).acquire_owned().await else {
                    break; // semaphore closed: only on shutdown
                };
                let state = state.clone();
                let listen_addr = listen_addr.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    set_nodelay_or_log(&stream);
                    let io = TokioIo::new(stream);
                    let peer_ip = peer.ip();
                    let conn = Arc::new(ConnState::new());
                    let service = {
                        let conn = Arc::clone(&conn);
                        hyper::service::service_fn(move |req| {
                            let state = state.clone();
                            let listen_addr = listen_addr.clone();
                            let conn = Arc::clone(&conn);
                            async move { handle(req, state, peer_ip, &listen_addr, conn).await }
                        })
                    };
                    let mut connection = std::pin::pin!(hyper::server::conn::http1::Builder::new()
                        // hyper is runtime-agnostic: without a timer
                        // installed, any timeout it is asked to honour panics
                        // the connection task rather than being ignored.
                        .timer(hyper_util::rt::TokioTimer::new())
                        .header_read_timeout(header_read_timeout)
                        .serve_connection(io, service));
                    let result = if idle_timeout.is_zero() {
                        connection.as_mut().await
                    } else {
                        tokio::select! {
                            result = connection.as_mut() => result,
                            _ = wait_until_idle(&conn, idle_timeout) => {
                                // Shutting down rather than dropping lets
                                // hyper finish a response still on the wire.
                                connection.as_mut().graceful_shutdown();
                                connection.as_mut().await
                            }
                        }
                    };
                    if let Err(err) = result {
                        tracing::debug!(r#type = "controller", %peer, error = %err, "connection error");
                    }
                });
            }
        });
    }

    wait_for_shutdown_signal().await;
    shutdown.notify_waiters();

    // 0 exits immediately.
    let grace_period = std::time::Duration::from_secs(state.config.php.shutdown.grace_period_seconds);
    let deadline = tokio::time::Instant::now() + grace_period;
    while state.in_flight.load(Relaxed) > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let remaining = state.in_flight.load(Relaxed);
    if remaining > 0 {
        tracing::warn!(
            r#type = "controller",
            ?grace_period,
            remaining,
            "grace period elapsed with request(s) still in flight, exiting anyway"
        );
    } else {
        tracing::info!(r#type = "controller", "all in-flight requests finished, exiting cleanly");
    }
    // Exiting closes our end of their sockets, which they already treat as
    // the signal to exit.
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
