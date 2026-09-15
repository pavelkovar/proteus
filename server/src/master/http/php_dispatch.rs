//! Request-body collection (with disk spillover) and dispatch to a PHP
//! worker.

use super::AppState;
use super::proxy::{resolve_https, resolve_server_name_port};
use super::routing::{ActionBody, DispatchResult, RequestContext, ResolvedScript};
use crate::ipc::data::{HeaderBlob, PhpRequest, RequestBody};
use crate::logging;
use crate::master::pool_manager::{DispatchOutcome, TempBodyFile};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode, Version};
use std::borrow::Cow;
use std::ffi::CString;
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use tokio::io::AsyncWriteExt;

/// Only a real extension method needs allocating.
fn method_cow(method: &Method) -> Cow<'static, str> {
    match method.as_str() {
        "GET" => Cow::Borrowed("GET"),
        "POST" => Cow::Borrowed("POST"),
        "PUT" => Cow::Borrowed("PUT"),
        "DELETE" => Cow::Borrowed("DELETE"),
        "HEAD" => Cow::Borrowed("HEAD"),
        "OPTIONS" => Cow::Borrowed("OPTIONS"),
        "CONNECT" => Cow::Borrowed("CONNECT"),
        "PATCH" => Cow::Borrowed("PATCH"),
        "TRACE" => Cow::Borrowed("TRACE"),
        other => Cow::Owned(other.to_string()),
    }
}

/// The fallback arm exists only because `Version` is `#[non_exhaustive]`.
fn version_str(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/1.1",
    }
}

/// Bodies above this spill to a temp file. Distinct from `max_body_size`,
/// which is the hard cap.
///
/// Bounded by `connection.max` times this, which is the heap a flood of
/// concurrent uploads can pin, not by the ring - spilling costs a file, an fd
/// handover and a read back, so the only reason not to go higher is that heap.
pub(crate) const BODY_MEMORY_THRESHOLD: usize = 63 * 1024; // 63 KiB

/// How much a spilled body gathers before reaching the file. Every write to
/// a `tokio::fs::File` is its own trip through the blocking pool, so writing
/// frames as they arrive costs one per frame, however small the client's are.
const SPILL_WRITE_THRESHOLD: usize = 64 * 1024;

static BODY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Latched off the first time the temp filesystem refuses `O_TMPFILE`, so the
/// named fallback is reached without re-trying a call that cannot start
/// succeeding.
static TMPFILE_USABLE: AtomicBool = AtomicBool::new(true);

/// pid plus a counter, so no randomness is needed to avoid collisions.
fn temp_body_path() -> PathBuf {
    let n = BODY_FILE_COUNTER.fetch_add(1, Relaxed);
    std::env::temp_dir().join(format!(
        "{}-body-{}-{n}",
        crate::APP_NAME,
        std::process::id()
    ))
}

/// A spill file with no name at any point, so there is no window in which
/// anything could open, replace or symlink it, and nothing to unlink or clean
/// up afterwards - closing the fd is the whole of it.
///
/// `None` means this filesystem has no `O_TMPFILE`.
fn open_unnamed_spill_file() -> Option<std::io::Result<std::fs::File>> {
    if !TMPFILE_USABLE.load(Relaxed) {
        return None;
    }
    let dir = std::env::temp_dir();
    let Ok(c_dir) = CString::new(dir.as_os_str().as_bytes()) else {
        return None;
    };
    // Readable as well as writable: the worker reads the body back through
    // this same open file description.
    let fd = unsafe {
        libc::open(
            c_dir.as_ptr(),
            libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
            0o600 as libc::c_uint,
        )
    };
    if fd >= 0 {
        return Some(Ok(unsafe { std::fs::File::from_raw_fd(fd) }));
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // No O_TMPFILE on this filesystem, or a kernel without it: both come
        // back as one of these, and neither changes while the process runs.
        Some(libc::EOPNOTSUPP) | Some(libc::EISDIR) | Some(libc::EINVAL) => {
            TMPFILE_USABLE.store(false, Relaxed);
            logging::warn!(
                r#type = "controller",
                dir = %dir.display(),
                error = %err,
                "O_TMPFILE unavailable, spilled request bodies now go through a named file"
            );
            None
        }
        _ => Some(Err(err)),
    }
}

/// Prefers an unnamed file; falls back to creating and immediately unlinking
/// a named one where the filesystem has no `O_TMPFILE`.
///
/// Both run on the blocking pool: opening resolves a path, and on a cold
/// dentry cache that is real I/O.
async fn open_spill_file() -> Result<tokio::fs::File, ()> {
    if let Some(result) = tokio::task::spawn_blocking(open_unnamed_spill_file)
        .await
        .expect("open_unnamed_spill_file cannot panic")
    {
        return match result {
            Ok(file) => Ok(tokio::fs::File::from_std(file)),
            Err(e) => {
                logging::warn!(r#type = "controller", error = %e, "failed creating an unnamed spilled request body file");
                Err(())
            }
        };
    }

    let path = temp_body_path();
    // `create_new` is O_EXCL, which is what refuses a symlink planted at this
    // predictable name. Readable because the worker reads the body back
    // through this same open file description.
    let mut open_options = tokio::fs::OpenOptions::new();
    open_options
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600);
    let file = match open_options.open(&path).await {
        Ok(f) => f,
        Err(e) => {
            logging::warn!(r#type = "controller", path = %path.display(), error = %e, "failed creating spilled request body file");
            return Err(());
        }
    };
    // Immediately: from here the body has no name either, so nothing can open
    // it, replace it, or need cleaning up.
    match tokio::fs::remove_file(&path).await {
        Ok(()) => {}
        // Someone else got there first, which is the state wanted.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        // The name outlives this request: nothing later knows it, and retrying
        // the call that just failed would not help.
        Err(e) => {
            logging::warn!(r#type = "controller", path = %path.display(), error = %e, "could not unlink the spilled request body, it will be left behind");
            return Err(());
        }
    }
    Ok(file)
}

enum CollectedBody {
    Inline(Vec<u8>),
    /// Counted as bytes arrive rather than stat()ed later, CONTENT_LENGTH
    /// being needed before the file is ever opened.
    Spilled {
        file: std::fs::File,
        len: u64,
    },
}

enum BodyCollectError {
    TooLarge,
    /// The body stopped arriving. Must fail the request rather than degrade
    /// to an empty one, which would hand the script a silently truncated body.
    Stalled,
    /// A body-stream read failed. Must fail the request rather than degrade
    /// to an empty body: PHP must never see a request that looks complete but
    /// isn't.
    Io,
    /// The spill file failed - this server's doing, not the client's, so the
    /// rest of the upload is drained before answering.
    Spill,
}

/// Buffers in memory only up to the spill threshold, never the whole body
/// just because `max_body_size` permits it.
///
/// `read_timeout` bounds the gap between two reads rather than the whole
/// upload, so an honest client on a slow link is never cut off for taking
/// its time.
async fn collect_body_streaming(
    body: Incoming,
    max_body_size: usize,
    read_timeout: Option<std::time::Duration>,
) -> Result<CollectedBody, BodyCollectError> {
    let mut limited = Limited::new(body, max_body_size);
    let collected = collect_frames(&mut limited, read_timeout).await;
    if matches!(collected, Err(BodyCollectError::Spill)) {
        drain_body(&mut limited, read_timeout).await;
    }
    collected
}

/// Reads off what is left of a body whose request has already failed, so the
/// error response reaches a client still uploading rather than the connection
/// closing under it. The same `Limited` still caps how much that can be.
async fn drain_body(limited: &mut Limited<Incoming>, read_timeout: Option<std::time::Duration>) {
    loop {
        let next = match read_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, limited.frame()).await {
                Ok(next) => next,
                Err(_) => return,
            },
            None => limited.frame().await,
        };
        if !matches!(next, Some(Ok(_))) {
            return;
        }
    }
}

async fn collect_frames(
    limited: &mut Limited<Incoming>,
    read_timeout: Option<std::time::Duration>,
) -> Result<CollectedBody, BodyCollectError> {
    let mut inline: Vec<u8> = Vec::new();
    let mut spilled: Option<tokio::fs::File> = None;
    let mut pending: Vec<u8> = Vec::new();
    let mut total_len: u64 = 0;

    loop {
        let next = match read_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, limited.frame()).await {
                Ok(next) => next,
                Err(_) => {
                    return Err(BodyCollectError::Stalled);
                }
            },
            None => limited.frame().await,
        };
        let frame = match next {
            None => break,
            Some(Ok(frame)) => frame,
            Some(Err(e)) if e.is::<http_body_util::LengthLimitError>() => {
                return Err(BodyCollectError::TooLarge);
            }
            Some(Err(_)) => {
                return Err(BodyCollectError::Io);
            }
        };
        let Ok(data) = frame.into_data() else {
            continue; // trailers, unused here
        };
        total_len += data.len() as u64;

        if let Some(file) = spilled.as_mut() {
            pending.extend_from_slice(&data);
            if pending.len() >= SPILL_WRITE_THRESHOLD {
                if let Err(e) = file.write_all(&pending).await {
                    logging::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
                    return Err(BodyCollectError::Spill);
                }
                pending.clear();
            }
            continue;
        }

        inline.extend_from_slice(&data);
        if inline.len() > BODY_MEMORY_THRESHOLD {
            let mut file = match open_spill_file().await {
                Ok(file) => file,
                Err(()) => return Err(BodyCollectError::Spill),
            };
            if let Err(e) = file.write_all(&inline).await {
                logging::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
                return Err(BodyCollectError::Spill);
            }
            inline.clear();
            inline.shrink_to_fit(); // don't hoard BODY_MEMORY_THRESHOLD bytes for nothing
            spilled = Some(file);
        }
    }

    // The tail is under the threshold by definition, so without this the file
    // would be short of the length already counted into `total_len`.
    let tail = match spilled.as_mut() {
        Some(file) => file.write_all(&pending).await,
        None => Ok(()),
    };
    if let Err(e) = tail {
        logging::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
        return Err(BodyCollectError::Spill);
    }

    Ok(match spilled {
        Some(mut file) => {
            // The worker inherits this open file description, offset included,
            // so it has to start at the beginning.
            use tokio::io::AsyncSeekExt as _;
            if let Err(e) = file.rewind().await {
                logging::warn!(r#type = "controller", error = %e, "failed rewinding spilled request body");
                return Err(BodyCollectError::Spill);
            }
            CollectedBody::Spilled {
                file: file.into_std().await,
                len: total_len,
            }
        }
        None => CollectedBody::Inline(inline),
    })
}

/// Marshals the request into what the worker needs. The cleanup guard
/// travels alongside and must not be dropped early.
pub(crate) async fn build_php_request(
    state: &AppState,
    req: Request<Incoming>,
    path: &str,
    resolved: ResolvedScript,
    ctx: RequestContext<'_>,
    max_body_size: usize,
) -> Result<(PhpRequest<'static>, Option<TempBodyFile>), Box<DispatchResult>> {
    let RequestContext {
        client_ip,
        listen_addr,
        server_addr,
        is_trusted_peer,
    } = ctx;

    let method = method_cow(req.method());
    let uri = req
        .uri()
        .path_and_query()
        .map(|pq| pq.to_string())
        .unwrap_or_else(|| path.to_string());
    // One sized allocation for the whole set; the per-entry term covers the
    // two NUL terminators.
    let header_bytes: usize = req
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + 2)
        .sum();
    let mut headers = HeaderBlob::with_capacity(header_bytes);
    for (name, value) in req.headers() {
        // A non-UTF-8 value cannot become a $_SERVER string.
        if let Ok(value) = value.to_str() {
            headers.push(name.as_str(), value);
        }
    }

    // The same trusted-peer gate as X-Forwarded-For.
    let (server_name, server_port) =
        resolve_server_name_port(req.headers(), is_trusted_peer, listen_addr);
    let server_protocol = Cow::Borrowed(version_str(req.version()));
    let https = resolve_https(req.headers(), is_trusted_peer);

    let body_read_timeout = (state.config.connection.body_read_timeout > 0)
        .then(|| std::time::Duration::from_secs(state.config.connection.body_read_timeout));
    let (body, body_cleanup) =
        match collect_body_streaming(req.into_body(), max_body_size, body_read_timeout).await {
            Ok(CollectedBody::Inline(bytes)) => (RequestBody::Inline(Cow::Owned(bytes)), None),
            Ok(CollectedBody::Spilled { file, len }) => (
                RequestBody::File { len },
                // Already unlinked: the worker gets this fd, nobody can reach the
                // file by name, and there is no ownership to hand over.
                Some(TempBodyFile::new(file)),
            ),
            Err(BodyCollectError::TooLarge) => {
                return Err(Box::new(php_error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    b"413 request body exceeds the configured limit\n",
                )));
            }
            Err(BodyCollectError::Stalled) => {
                return Err(Box::new(php_error_response(
                    StatusCode::REQUEST_TIMEOUT,
                    b"408 request body stopped arriving\n",
                )));
            }
            Err(BodyCollectError::Io | BodyCollectError::Spill) => {
                return Err(Box::new(php_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    b"500 Internal Server Error\n",
                )));
            }
        };

    Ok((
        PhpRequest {
            script_path: Cow::Owned(resolved.script_path),
            document_root: Cow::Owned(resolved.document_root),
            script_name: Cow::Owned(resolved.script_name),
            path_info: Cow::Owned(resolved.path_info),
            method,
            uri: Cow::Owned(uri),
            headers,
            client_ip: client_ip.ip(),
            body,
            server_name: Cow::Owned(server_name),
            server_addr,
            server_port,
            server_protocol,
            https,
        },
        body_cleanup,
    ))
}

pub(crate) async fn dispatch_php(
    state: &AppState,
    req: PhpRequest<'static>,
    body_cleanup: Option<TempBodyFile>,
) -> DispatchResult {
    // Shared rather than borrowed: it is encoded on another task, and a
    // retry re-sends it without re-encoding.
    let req = std::sync::Arc::new(req);
    match state.pool.dispatch(&req, body_cleanup).await {
        // Still arriving; passed through rather than buffered.
        DispatchOutcome::Ok(resp) => {
            let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
            DispatchResult::new(
                ActionBody::PhpStream {
                    status,
                    headers: resp.headers,
                    body: resp.body,
                },
                "php",
                resp.worker_pid,
            )
        }
        DispatchOutcome::Timeout => {
            php_error_response(StatusCode::GATEWAY_TIMEOUT, b"504 Gateway Timeout\n")
        }
        DispatchOutcome::QueueTimeout => php_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 Service Unavailable\n",
        ),
        // The head cap bounds everything a client controls, so a frame that
        // still overflows is this deployment's paths, not the request.
        DispatchOutcome::RequestTooLarge => php_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"500 Internal Server Error\n",
        ),
        DispatchOutcome::Failed => php_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"500 Internal Server Error\n",
        ),
    }
}

/// Shared shape for the failure arms.
fn php_error_response(status: StatusCode, body: &'static [u8]) -> DispatchResult {
    DispatchResult::new(ActionBody::plain(status, body), "php", 0)
}
