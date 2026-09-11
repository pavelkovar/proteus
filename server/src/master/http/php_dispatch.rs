//! Request-body collection (with disk spillover) and dispatch to a PHP
//! worker.

use super::AppState;
use super::proxy::{resolve_https, resolve_server_name_port};
use super::routing::{ActionBody, DispatchResult, RequestContext, ResolvedScript};
use crate::ipc::data::{HeaderBlob, PhpRequest, RequestBody};
use crate::master::pool_manager::{DispatchOutcome, TempBodyFile};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode, Version};
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
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
const BODY_MEMORY_THRESHOLD: usize = 256 * 1024; // 256 KiB

/// How much a spilled body gathers before reaching the file. Every write to
/// a `tokio::fs::File` is its own trip through the blocking pool, so writing
/// frames as they arrive costs one per frame, however small the client's are.
const SPILL_WRITE_THRESHOLD: usize = 64 * 1024;

static BODY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// pid plus a counter, so no randomness is needed to avoid collisions.
fn temp_body_path() -> PathBuf {
    let n = BODY_FILE_COUNTER.fetch_add(1, Relaxed);
    std::env::temp_dir().join(format!(
        "{}-body-{}-{n}",
        crate::APP_NAME,
        std::process::id()
    ))
}

enum CollectedBody {
    Inline(Vec<u8>),
    /// Counted as bytes arrive rather than stat()ed later, CONTENT_LENGTH
    /// being needed before the file is ever opened.
    Spilled {
        path: PathBuf,
        len: u64,
    },
}

enum BodyCollectError {
    TooLarge,
    /// The body stopped arriving. Must fail the request rather than degrade
    /// to an empty one, which would hand the script a silently truncated body.
    Stalled,
    /// A body-stream read or spill-file write failed. Must fail the request
    /// rather than degrade to an empty body: PHP must never see a request
    /// that looks complete but isn't.
    Io,
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
    let mut inline: Vec<u8> = Vec::new();
    let mut spilled: Option<(tokio::fs::File, PathBuf)> = None;
    let mut pending: Vec<u8> = Vec::new();
    let mut total_len: u64 = 0;

    let cleanup_on_error = |spilled: &Option<(tokio::fs::File, PathBuf)>| {
        if let Some((_, path)) = spilled {
            let path = path.clone();
            tokio::spawn(async move {
                let _ = tokio::fs::remove_file(&path).await;
            });
        }
    };

    loop {
        let next = match read_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, limited.frame()).await {
                Ok(next) => next,
                Err(_) => {
                    cleanup_on_error(&spilled);
                    return Err(BodyCollectError::Stalled);
                }
            },
            None => limited.frame().await,
        };
        let frame = match next {
            None => break,
            Some(Ok(frame)) => frame,
            Some(Err(e)) if e.is::<http_body_util::LengthLimitError>() => {
                cleanup_on_error(&spilled);
                return Err(BodyCollectError::TooLarge);
            }
            Some(Err(_)) => {
                cleanup_on_error(&spilled);
                return Err(BodyCollectError::Io);
            }
        };
        let Ok(data) = frame.into_data() else {
            continue; // trailers, unused here
        };
        total_len += data.len() as u64;

        if let Some((file, _)) = spilled.as_mut() {
            pending.extend_from_slice(&data);
            if pending.len() >= SPILL_WRITE_THRESHOLD {
                if let Err(e) = file.write_all(&pending).await {
                    tracing::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
                    cleanup_on_error(&spilled);
                    return Err(BodyCollectError::Io);
                }
                pending.clear();
            }
            continue;
        }

        inline.extend_from_slice(&data);
        if inline.len() > BODY_MEMORY_THRESHOLD {
            let path = temp_body_path();
            // The name is predictable, so O_EXCL is what refuses a planted
            // symlink; the mode keeps it from being briefly world-readable
            // mid-upload.
            let mut open_options = tokio::fs::OpenOptions::new();
            open_options.write(true).create_new(true).mode(0o600);
            let mut file = match open_options.open(&path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(r#type = "controller", path = %path.display(), error = %e, "failed creating spilled request body file");
                    return Err(BodyCollectError::Io);
                }
            };
            if let Err(e) = file.write_all(&inline).await {
                tracing::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
                let _ = tokio::fs::remove_file(&path).await;
                return Err(BodyCollectError::Io);
            }
            inline.clear();
            inline.shrink_to_fit(); // don't hoard BODY_MEMORY_THRESHOLD bytes for nothing
            spilled = Some((file, path));
        }
    }

    // The tail is under the threshold by definition, so without this the file
    // would be short of the length already counted into `total_len`.
    let tail = match spilled.as_mut() {
        Some((file, _)) => file.write_all(&pending).await,
        None => Ok(()),
    };
    if let Err(e) = tail {
        tracing::warn!(r#type = "controller", error = %e, "failed writing spilled request body");
        cleanup_on_error(&spilled);
        return Err(BodyCollectError::Io);
    }

    Ok(match spilled {
        Some((_, path)) => CollectedBody::Spilled {
            path,
            len: total_len,
        },
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
        is_trusted_peer,
    } = ctx;

    let method = method_cow(req.method());
    let uri = req
        .uri()
        .path_and_query()
        .map(|pq| pq.to_string())
        .unwrap_or_else(|| path.to_string());
    let query_string = req.uri().query().unwrap_or("").to_string();
    let content_type = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
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
    let (body, body_cleanup) = match collect_body_streaming(
        req.into_body(),
        max_body_size,
        body_read_timeout,
    )
    .await
    {
        Ok(CollectedBody::Inline(bytes)) => (RequestBody::Inline(Cow::Owned(bytes)), None),
        Ok(CollectedBody::Spilled { path, len }) => {
            // Master created the file as itself, so it must be handed over.
            // Skipped when the worker shares master's identity.
            let (uid, gid) = state.pool.worker_uid_gid();
            if (uid, gid)
                != (
                    nix::unistd::getuid().as_raw(),
                    nix::unistd::getgid().as_raw(),
                )
                && let Err(e) = nix::unistd::chown(
                    &path,
                    Some(nix::unistd::Uid::from_raw(uid)),
                    Some(nix::unistd::Gid::from_raw(gid)),
                )
            {
                tracing::warn!(r#type = "controller", path = %path.display(), uid, gid, error = %e, "failed to chown spilled body file, failing the request");
                let _cleanup = TempBodyFile::new(path);
                return Err(Box::new(DispatchResult::new(
                    ActionBody::Buffered {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        body: b"500 failed to prepare request body\n".to_vec(),
                        headers: HeaderBlob::default(),
                    },
                    "php",
                    0,
                )));
            }
            (
                RequestBody::File {
                    path: Cow::Owned(path.to_string_lossy().into_owned()),
                    len,
                },
                Some(TempBodyFile::new(path)),
            )
        }
        Err(BodyCollectError::TooLarge) => {
            return Err(Box::new(DispatchResult::new(
                ActionBody::Buffered {
                    status: StatusCode::PAYLOAD_TOO_LARGE,
                    body: b"413 request body exceeds the configured limit\n".to_vec(),
                    headers: HeaderBlob::default(),
                },
                "php",
                0,
            )));
        }
        Err(BodyCollectError::Stalled) => {
            return Err(Box::new(DispatchResult::new(
                ActionBody::Buffered {
                    status: StatusCode::REQUEST_TIMEOUT,
                    body: b"408 request body stopped arriving\n".to_vec(),
                    headers: HeaderBlob::default(),
                },
                "php",
                0,
            )));
        }
        Err(BodyCollectError::Io) => {
            return Err(Box::new(DispatchResult::new(
                ActionBody::Buffered {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: b"500 failed to read request body\n".to_vec(),
                    headers: HeaderBlob::default(),
                },
                "php",
                0,
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
            query_string: Cow::Owned(query_string),
            content_type: Cow::Owned(content_type),
            headers,
            client_ip: client_ip.ip(),
            body,
            server_name: Cow::Owned(server_name),
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
        DispatchOutcome::Timeout => php_error_response(
            StatusCode::GATEWAY_TIMEOUT,
            b"504 worker did not respond in time\n",
        ),
        DispatchOutcome::QueueTimeout => php_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            b"503 no worker capacity available\n",
        ),
        DispatchOutcome::Failed => php_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"500 worker dispatch failed\n",
        ),
    }
}

/// Shared shape for the failure arms.
fn php_error_response(status: StatusCode, body: &'static [u8]) -> DispatchResult {
    DispatchResult::new(
        ActionBody::Buffered {
            status,
            body: body.to_vec(),
            headers: HeaderBlob::default(),
        },
        "php",
        0,
    )
}
