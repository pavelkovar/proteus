//! HTTP entry point: connection accept and drain, request handling, client
//! IP resolution, access log.

mod access_log;
mod compression;
mod conditional;
mod connection;
mod php_dispatch;
mod proxy;
mod range;
mod rate_limit;
mod response;
mod routing;
mod shutdown;

use access_log::{BodyOutcome, GuardedBody, PendingAccessLog};
use conditional::*;
use connection::{ConnBusyGuard, ConnState, ConnTimeouts, wait_until_idle};
use proxy::*;
use rate_limit::RateLimiter;
use response::*;
use routing::*;
pub use shutdown::Shutdown;
use shutdown::wait_for_shutdown_signal;

use crate::config::{Config, RouteActionConfig};
use crate::logging;
use crate::master::pool_manager::PoolManager;
use crate::utils::fs_cache::FsCache;
use crate::utils::gauge::{Gauge, GaugeGuard};
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

pub struct AppState {
    /// `Arc` so a background finish-watch task can hold its own reference.
    pub pool: Arc<PoolManager>,
    pub config: Config,
    /// Polled after SIGTERM to know when it is safe to stop. `Arc` because a
    /// guard outlives `handle`, riding the response body to its last frame.
    pub in_flight: Arc<Gauge>,
    /// Existence and type only, never content.
    pub fs_cache: FsCache,
    /// Decided once at startup, so a request need not scan every route just
    /// to learn whether it has to resolve a Host at all.
    pub uses_host_matching: bool,
    /// `None` when `rate_limit` is absent from config - the feature is off.
    rate_limiter: Option<RateLimiter>,
}

impl AppState {
    pub fn new(pool: Arc<PoolManager>, mut config: Config, fs_cache: FsCache) -> Self {
        let uses_host_matching = config.routes.iter().any(|r| !r.matcher.host.is_empty());
        // Taken rather than cloned: nothing else needs `config.rate_limit`
        // once it has become `state.rate_limiter`.
        let rate_limiter = config
            .rate_limit
            .take()
            .map(|rl| RateLimiter::new(rl.requests, rl.period_seconds, rl.user_agent));
        AppState {
            pool,
            config,
            in_flight: Arc::default(),
            fs_cache,
            uses_host_matching,
            rate_limiter,
        }
    }
}

/// Owns its handle rather than borrowing: it moves into a body that outlives
/// `handle`.
type InFlightGuard = GaugeGuard<Arc<Gauge>>;

type ResponseBody = BoxBody<Bytes, std::io::Error>;

/// Everything the end of a request needs, whichever of `handle`'s exits it
/// takes: the guards that must outlive the response, and the log fields known
/// before any routing happens.
struct Ending {
    client_ip: ClientIdentity,
    method: Method,
    uri: hyper::Uri,
    start: Instant,
    in_flight: InFlightGuard,
    conn_busy: ConnBusyGuard,
}

type Handled = Result<Response<ResponseBody>, std::convert::Infallible>;

impl Ending {
    /// Hands the response the guards and the log entry, which then ride it to
    /// its last frame.
    fn finish(
        self,
        resp: Response<ResponseBody>,
        action: &'static str,
        worker_pid: u32,
        php_target: Option<Arc<str>>,
        outcome: BodyOutcome,
    ) -> Handled {
        let log = PendingAccessLog {
            client_ip: self.client_ip,
            method: self.method,
            uri: self.uri,
            status: resp.status(),
            start: self.start,
            action,
            worker_pid,
            php_target,
        };
        Ok(resp.map(|inner| {
            GuardedBody {
                inner,
                _guard: self.in_flight,
                _conn: self.conn_busy,
                log: Some(log),
                outcome,
            }
            .boxed()
        }))
    }

    /// A refusal `handle` built itself, so no worker and no target were
    /// involved and the body is already complete.
    fn reject(self, resp: Response<ResponseBody>, action: &'static str) -> Handled {
        self.finish(resp, action, 0, None, BodyOutcome::Complete)
    }

    /// The plain-text case, where nothing but the status and the message vary.
    fn reject_plain(
        self,
        status: StatusCode,
        body: &'static [u8],
        action: &'static str,
    ) -> Handled {
        let resp = build_response(
            status,
            body.to_vec(),
            &crate::ipc::data::HeaderBlob::default(),
        );
        self.reject(resp, action)
    }
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
    peer: Peer,
    listen_addr: &str,
    server_addr: std::net::IpAddr,
    conn: Arc<ConnState>,
) -> Result<Response<ResponseBody>, std::convert::Infallible> {
    // Ahead of path decoding/routing/body collection: resolving these needs
    // no work beyond the headers already in hand, so a client about to be
    // rate-limited or otherwise rejected never pays for any of that first.
    let is_trusted_peer = peer.is_trusted_proxy();
    let client_ip = resolve_client_ip(peer, req.headers(), &state.config.trusted_proxies);
    let ending = Ending {
        client_ip,
        method: req.method().clone(),
        // A refcount bump, not a copy. Needed because `req` is moved below,
        // so the path cannot simply borrow from it.
        uri: req.uri().clone(),
        start: Instant::now(),
        in_flight: InFlightGuard::new(Arc::clone(&state.in_flight)),
        conn_busy: ConnBusyGuard::new(conn),
    };

    if let Some(limiter) = &state.rate_limiter {
        // The residual risk the gate cannot cover: a trusted proxy that
        // forwards a client-chosen X-Forwarded-For verbatim lets that client
        // rotate past this limiter entirely.
        let user_agent = req
            .headers()
            .get(hyper::header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if limiter.should_limit(user_agent) && !limiter.check(client_ip) {
            let mut resp = build_response(
                StatusCode::TOO_MANY_REQUESTS,
                b"429 too many requests\n".to_vec(),
                &crate::ipc::data::HeaderBlob::default(),
            );
            // A fixed value from config rather than the bucket's own precise
            // refill time: simple, and accurate enough for clients that
            // honor it.
            if let Ok(value) =
                hyper::header::HeaderValue::from_str(&limiter.period_seconds().to_string())
            {
                resp.headers_mut().insert(hyper::header::RETRY_AFTER, value);
            }
            return ending.reject(resp, "rate-limited");
        }
    }

    // Decoded once, for routing, the filesystem and PATH_INFO alike.
    // REQUEST_URI keeps the raw form, as every other SAPI reports it.
    let decoded_path = match percent_decode_path(ending.uri.path()) {
        Ok(path) => path,
        Err(reason) => {
            logging::debug!(
                r#type = "controller",
                ?reason,
                raw_path = ending.uri.path(),
                "rejected an undecodable request path"
            );
            return ending.reject_plain(StatusCode::BAD_REQUEST, b"400 invalid path\n", "rejected");
        }
    };
    let path = decoded_path.as_ref();
    // Shares the buffer rather than copying the string out.
    let accept_encoding = req.headers().get(hyper::header::ACCEPT_ENCODING).cloned();
    let accept_encoding = accept_encoding
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Skipped unless some route matches on it.
    let host_for_matching = if state.uses_host_matching {
        resolve_server_name_port(req.headers(), is_trusted_peer, listen_addr)
            .0
            .to_ascii_lowercase()
    } else {
        String::new()
    };
    let decision = match_route(
        &state.config,
        path,
        ending.method.as_str(),
        &host_for_matching,
    );
    // Only Static consults these.
    let is_static_route = matches!(
        &decision,
        RouteDecision::Matched {
            action: RouteActionConfig::Static { .. },
            ..
        }
    );
    let conditional = if is_static_route {
        ConditionalHeaders::from_headers(req.headers())
    } else {
        ConditionalHeaders::default()
    };

    let compression = CompressionParams {
        accept_encoding,
        min_size_bytes: state.config.compression.min_size_bytes,
        mime_types: &state.config.compression.mime_types,
    };

    // Once, before any routing, rather than per match arm: a new arm added
    // without repeating the check would silently admit traversal.
    if path_escapes_root(path) {
        return ending.reject_plain(StatusCode::BAD_REQUEST, b"400 invalid path\n", "rejected");
    }

    let ctx = RequestContext {
        client_ip,
        listen_addr,
        server_addr,
        is_trusted_peer,
    };
    let DispatchResult {
        action_body,
        log_action,
        worker_pid,
        php_target,
    } = match decision {
        RouteDecision::Matched { action } => dispatch_action(&state, action, req, path, ctx).await,
        RouteDecision::NoMatch => DispatchResult::new(ActionBody::not_found(), "none", 0),
    };

    let resp = match action_body {
        ActionBody::StaticFile {
            file,
            meta,
            candidate,
        } => {
            build_static_response(
                file,
                &meta,
                &candidate,
                path,
                compression,
                &conditional,
                ending.method == Method::HEAD,
            )
            .await
        }
        ActionBody::PhpStream {
            status,
            headers,
            body,
        } => build_php_stream_response(status, &headers, body, compression),
        ActionBody::Buffered {
            status,
            body,
            headers,
        } => build_response(status, body, &headers),
    };
    // `Aborted` until proven otherwise, so a body dropped before its end
    // records the client hanging up.
    ending.finish(
        resp,
        log_action,
        worker_pid,
        php_target,
        BodyOutcome::Aborted,
    )
}

/// Holds the connection in the kernel until the request arrives, so a peer
/// that only completes the handshake costs no accept or connection slot.
/// One second: longer would blind `header_read_timeout` to the wait.
fn set_defer_accept_or_log(listener: &TcpListener, listen: &str) {
    use std::os::fd::AsRawFd;
    let secs: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_DEFER_ACCEPT,
            std::ptr::from_ref(&secs).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        logging::warn!(
            r#type = "controller",
            %listen,
            error = %std::io::Error::last_os_error(),
            "setsockopt(TCP_DEFER_ACCEPT) failed, an idle peer now costs a connection slot"
        );
    }
}

/// Traffic here is small and latency-sensitive, never the bulk transfer
/// Nagle helps. Left on, Nagle plus the client's delayed ACK is the classic
/// fixed-40ms-per-request bug.
fn set_nodelay_or_log(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        logging::warn!(r#type = "controller", error = %e, "set_nodelay failed");
    }
}

/// One more listening socket for `listen`, sharing the port with whatever is
/// already bound to it. The kernel hands each connection to exactly one of
/// them, so accepting stops being one thread's work.
pub fn reuseport_listener(listen: &str) -> std::io::Result<std::net::TcpListener> {
    let addr: std::net::SocketAddr = listen.parse().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{listen}: {e}"))
    })?;
    let sock = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

/// Accepts on this core's own share of every listening address and serves what
/// it accepts, all on one thread. Distributing connections in user space
/// instead measured no better and cost a cross-thread handoff.
pub async fn serve_core(
    listeners: Vec<(std::net::TcpListener, Arc<str>)>,
    state: Arc<AppState>,
    shutdown: Shutdown,
    exit: Shutdown,
    connection_slots: Arc<tokio::sync::Semaphore>,
) {
    let mut accepting = Vec::with_capacity(listeners.len());
    for (listener, listen_addr) in listeners {
        accepting.push(tokio::spawn(accept_loop(
            listener,
            listen_addr,
            Arc::clone(&state),
            shutdown.clone(),
            Arc::clone(&connection_slots),
        )));
    }
    for task in accepting {
        let _ = task.await;
    }
    // Connections outlive the accept loops: dropping this runtime drops their
    // tasks, so it has to stay until master says the drain is over.
    let mut exit = exit;
    exit.wait().await;
}

async fn accept_loop(
    listener: std::net::TcpListener,
    listen_addr: Arc<str>,
    state: Arc<AppState>,
    mut shutdown: Shutdown,
    connection_slots: Arc<tokio::sync::Semaphore>,
) {
    listener
        .set_nonblocking(true)
        .expect("a listening socket must go non-blocking");
    let listener = match TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(e) => {
            logging::error!(r#type = "controller", %listen_addr, error = %e, "registering a listening socket failed");
            return;
        }
    };
    set_defer_accept_or_log(&listener, &listen_addr);
    let timeouts = ConnTimeouts::from(&state);
    loop {
        let (stream, peer) = tokio::select! {
            result = listener.accept() => match result {
                Ok(v) => v,
                Err(e) => {
                    logging::warn!(r#type = "controller", error = %e, "accept failed");
                    continue;
                }
            },
            _ = shutdown.wait() => break,
        };
        // Before spawning, so a flood waits in the kernel's bounded accept
        // backlog rather than piling up as tasks holding sockets. Raced against
        // shutdown like every wait here: live keep-alives make it unbounded.
        let slot = tokio::select! {
            result = Arc::clone(&connection_slots).acquire_owned() => match result {
                Ok(slot) => slot,
                Err(_) => break, // semaphore closed: only on shutdown
            },
            _ = shutdown.wait() => break,
        };
        let state = Arc::clone(&state);
        let listen_addr = Arc::clone(&listen_addr);
        tokio::spawn(async move {
            let _slot = slot;
            serve_one_connection(stream, peer, listen_addr, state, timeouts).await;
        });
    }
    logging::info!(r#type = "controller", %listen_addr, "no longer accepting new connections");
}

/// What one request's head may total, on every listener. The frame carrying it
/// repeats the URI and Host, so the ring bounds this at roughly half its own
/// size once an inline body and the resolved paths are allowed for.
const MAX_REQUEST_HEAD: usize = 80 * 1024;

// The head twice over - the URI and Host each get a field of their own beside
// the blob - plus an inline body and the resolved paths. Checked, so tuning
// any of them alone breaks the build rather than production.
const _: () = assert!(
    2 * MAX_REQUEST_HEAD + php_dispatch::BODY_MEMORY_THRESHOLD + 16 * 1024
        <= crate::ipc::shm::REQUEST_RING_CAPACITY - 4,
    "a request at the head cap must fit one ring frame, paths and body included"
);

async fn serve_one_connection(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    listen_addr: Arc<str>,
    state: Arc<AppState>,
    timeouts: ConnTimeouts,
) {
    set_nodelay_or_log(&stream);
    // Before the stream is consumed. A connected socket always has one, so the
    // fallback only keeps SERVER_ADDR present rather than meaningful.
    let server_addr = stream
        .local_addr()
        .map_or(std::net::IpAddr::from([0, 0, 0, 0]), |addr| addr.ip());
    let io = TokioIo::new(stream);
    let peer_identity = Peer::resolve(peer.ip(), &state.config.trusted_proxies);
    let conn = Arc::new(ConnState::new());
    let service = {
        let conn = Arc::clone(&conn);
        hyper::service::service_fn(move |req| {
            let state = state.clone();
            let listen_addr = listen_addr.clone();
            let conn = Arc::clone(&conn);
            async move { handle(req, state, peer_identity, &listen_addr, server_addr, conn).await }
        })
    };
    let mut connection = std::pin::pin!(
        hyper::server::conn::http1::Builder::new()
            .max_buf_size(MAX_REQUEST_HEAD)
            // hyper is runtime-agnostic: without a timer installed, any timeout
            // it is asked to honour panics the connection task rather than
            // being ignored.
            .timer(hyper_util::rt::TokioTimer::new())
            .header_read_timeout(timeouts.header_read)
            .serve_connection(io, service)
    );
    let result = if timeouts.idle.is_zero() {
        connection.as_mut().await
    } else {
        tokio::select! {
            result = connection.as_mut() => result,
            _ = wait_until_idle(&conn, timeouts.idle) => {
                // Shutting down rather than dropping lets hyper finish a
                // response still on the wire.
                connection.as_mut().graceful_shutdown();
                connection.as_mut().await
            }
        }
    };
    if let Err(err) = result {
        logging::debug!(r#type = "controller", %peer, error = %err, "connection error");
    }
}

/// Deliberately a separate listener from public traffic.
async fn serve_status(listen: String, state: Arc<AppState>) {
    let header_read_timeout =
        std::time::Duration::from_secs(state.config.connection.header_read_timeout);
    let listener = TcpListener::bind(&listen)
        .await
        .expect("status bind failed");
    logging::info!(r#type = "controller", %listen, "status endpoint listening");
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                logging::warn!(r#type = "controller", error = %e, "status accept failed");
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
                .max_buf_size(MAX_REQUEST_HEAD)
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(header_read_timeout)
                .serve_connection(io, service)
                .await;
        });
    }
}

/// Returns once in-flight requests have finished or the grace period runs out.
pub async fn serve_control(state: Arc<AppState>, shutdown: tokio::sync::watch::Sender<bool>) {
    let status_listen = state.config.status.listen.clone();
    tokio::spawn(serve_status(status_listen, state.clone()));

    wait_for_shutdown_signal().await;
    // Latched, so an accept loop blocked elsewhere still sees it when it
    // next looks.
    let _ = shutdown.send(true);

    // 0 exits immediately.
    let grace_period =
        std::time::Duration::from_secs(state.config.php.shutdown.grace_period_seconds);
    let deadline = tokio::time::Instant::now() + grace_period;
    while state.in_flight.get() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let remaining = state.in_flight.get();
    if remaining > 0 {
        logging::warn!(
            r#type = "controller",
            ?grace_period,
            remaining,
            "grace period elapsed with request(s) still in flight, exiting anyway"
        );
    } else {
        logging::info!(
            r#type = "controller",
            "all in-flight requests finished, exiting cleanly"
        );
    }
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
