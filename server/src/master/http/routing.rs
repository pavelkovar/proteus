//! Route matching and the config-action walk, including `fallback` chains
//! and PHP target resolution.

use super::AppState;
use super::fs_cache::{FsCache, FsKind};
use super::php_dispatch::{build_php_request, dispatch_php};
use super::proxy::ClientIdentity;
use crate::config::{Config, RouteActionConfig, extension_is_listed};
use crate::ipc::data::HeaderBlob;
use crate::logging;
use crate::master::pool_manager::BodyStream;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use nix::fcntl::{PosixFadviseAdvice, posix_fadvise};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

/// Below this the hint costs more than it buys: the kernel's own default
/// readahead window already covers a file this small.
const MIN_READ_AHEAD: u64 = 128 * 1024;

/// Latched off the first time the kernel rejects the call itself: without
/// it every request would spend a syscall that cannot start succeeding.
static OPENAT2_USABLE: AtomicBool = AtomicBool::new(true);

/// `open` that gives up instead of waiting when the path is not already in
/// the kernel's lookup cache - the bargain `pread_nowait` makes for reads.
///
/// `None` means "hand it to the blocking pool". Every other error is the
/// real answer and comes back as `Some(Err(..))`, so a miss is not opened
/// twice.
pub(crate) fn open_cached(path: &Path) -> Option<std::io::Result<std::fs::File>> {
    if !OPENAT2_USABLE.load(Relaxed) {
        return None;
    }
    // An interior NUL names no file; let the fallback raise the error.
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // Zeroed rather than built field by field: `open_how` is the kernel's
    // extensible-struct ABI, where an unset field must read as zero.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_RDONLY | libc::O_CLOEXEC) as u64;
    how.resolve = libc::RESOLVE_CACHED;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            libc::AT_FDCWD,
            c_path.as_ptr(),
            std::ptr::from_ref(&how),
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if rc >= 0 {
        return Some(Ok(unsafe { std::fs::File::from_raw_fd(rc as libc::c_int) }));
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) => None,
        // No `openat2` (pre-5.6), no `RESOLVE_CACHED` (pre-5.12), or a
        // seccomp profile that refuses the call.
        Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EPERM) | Some(libc::E2BIG) => {
            OPENAT2_USABLE.store(false, Relaxed);
            logging::warn!(
                r#type = "controller",
                error = %err,
                "openat2(RESOLVE_CACHED) unavailable, every static open now goes through the blocking pool"
            );
            None
        }
        _ => Some(Err(err)),
    }
}

#[cfg(test)]
pub(crate) fn openat2_usable_for_test() -> bool {
    OPENAT2_USABLE.load(Relaxed)
}

/// Whatever opened the fd, this decides what it promises.
pub(crate) fn stat_and_advise(
    file: std::fs::File,
) -> std::io::Result<(std::fs::File, std::fs::Metadata)> {
    let meta = file.metadata()?;
    // Every path this fd takes reads it in order, whole file or range.
    if !meta.is_dir() && meta.len() > MIN_READ_AHEAD {
        let _ = posix_fadvise(&file, 0, 0, PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL);
    }
    Ok((file, meta))
}

/// Finds the matching route only; walking a `fallback` chain is separate.
#[derive(Debug, PartialEq)]
pub(crate) enum RouteDecision<'a> {
    Matched { action: &'a RouteActionConfig },
    NoMatch,
}

pub(crate) fn match_route<'a>(
    cfg: &'a Config,
    path: &str,
    method: &str,
    host: &str,
) -> RouteDecision<'a> {
    for route in &cfg.routes {
        if route.matcher.matches(path, method, host) {
            return RouteDecision::Matched {
                action: &route.action,
            };
        }
    }
    RouteDecision::NoMatch
}

/// Rejects `..` before `Path::join`, which would not confine it.
/// Component-wise rather than canonicalize-and-compare, since canonicalize
/// needs the target to exist and would misreport traversal as a 404.
///
/// MUST run on the decoded path, or it misses `%2e%2e` entirely.
pub(crate) fn path_escapes_root(path: &str) -> bool {
    std::path::Path::new(path)
        .components()
        .any(|c| c == std::path::Component::ParentDir)
}

/// Why a request target was refused outright.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PathDecodeError {
    /// A `%` not followed by two hex digits.
    Malformed,
    /// Decoding these would let a client invent path segments after routing
    /// and traversal checks had run on a different shape of path. Rejecting
    /// beats leaving them encoded, which gives PHP a `PATH_INFO` disagreeing
    /// with the file actually opened.
    EncodedSeparator,
    /// Would truncate any C string built from it downstream.
    Nul,
    /// Nothing downstream can carry them, and guessing an encoding would be
    /// its own bug.
    NotUtf8,
}

/// Everything filesystem- and CGI-facing needs the decoded form, and routing
/// matches on it too, so a pattern cannot be dodged by spelling a character
/// as `%xx`. `REQUEST_URI` keeps the raw form, as every other SAPI does.
pub(crate) fn percent_decode_path(
    path: &str,
) -> Result<std::borrow::Cow<'_, str>, PathDecodeError> {
    if !path.contains('%') {
        return Ok(std::borrow::Cow::Borrowed(path));
    }
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        if bytes.len() < i + 3 {
            return Err(PathDecodeError::Malformed);
        }
        let (hi, lo) = (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2]));
        let (Some(hi), Some(lo)) = (hi, lo) else {
            return Err(PathDecodeError::Malformed);
        };
        match (hi << 4) | lo {
            0 => return Err(PathDecodeError::Nul),
            b'/' | b'\\' => return Err(PathDecodeError::EncodedSeparator),
            byte => out.push(byte),
        }
        i += 3;
    }
    String::from_utf8(out)
        .map(std::borrow::Cow::Owned)
        .map_err(|_| PathDecodeError::NotUtf8)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Per-connection facts, as one `Copy` struct rather than positional args.
#[derive(Clone, Copy)]
pub(crate) struct RequestContext<'a> {
    pub(crate) client_ip: ClientIdentity,
    pub(crate) listen_addr: &'a str,
    pub(crate) is_trusted_peer: bool,
}

/// A matched action, not yet a response.
pub(crate) enum ActionBody {
    StaticFile {
        file: std::fs::File,
        meta: std::fs::Metadata,
        candidate: PathBuf,
    },
    /// Headers landed; the body may still be streaming from the worker.
    PhpStream {
        status: StatusCode,
        headers: HeaderBlob<'static>,
        body: BodyStream,
    },

    Buffered {
        status: StatusCode,
        body: Vec<u8>,
        headers: HeaderBlob<'static>,
    },
}

impl ActionBody {
    pub(crate) fn not_found() -> Self {
        ActionBody::Buffered {
            status: StatusCode::NOT_FOUND,
            body: b"404 not found\n".to_vec(),
            headers: HeaderBlob::default(),
        }
    }
}

/// `worker_pid` is 0 and `php_target` `None` for non-PHP outcomes; a real pid
/// is never 0.
pub(crate) struct DispatchResult {
    pub(crate) action_body: ActionBody,
    pub(crate) log_action: &'static str,
    pub(crate) worker_pid: u32,
    /// `Option` rather than an empty `Arc<str>`, which allocates just to say
    /// "no target".
    pub(crate) php_target: Option<Arc<str>>,
}

impl DispatchResult {
    pub(crate) fn new(action_body: ActionBody, log_action: &'static str, worker_pid: u32) -> Self {
        DispatchResult {
            action_body,
            log_action,
            worker_pid,
            php_target: None,
        }
    }
}

/// Iterative rather than recursive, which keeps `req` owned and needs no
/// `Box::pin`. Script resolution runs before the body is buffered, so a
/// target miss 404s without touching it.
pub(crate) async fn dispatch_action(
    state: &AppState,
    action: &RouteActionConfig,
    req: Request<Incoming>,
    path: &str,
    ctx: RequestContext<'_>,
) -> DispatchResult {
    let mut current = action;
    loop {
        match current {
            RouteActionConfig::Static { root, fallback } => {
                let candidate = std::path::Path::new(root).join(path.trim_start_matches('/'));

                // Only a cached miss or directory is actionable; a File
                // verdict still needs the real open below.
                if matches!(
                    state.fs_cache.get(&candidate),
                    Some(FsKind::Dir) | Some(FsKind::Missing)
                ) {
                    match fallback {
                        Some(next) => {
                            current = next;
                            continue;
                        }
                        None => return DispatchResult::new(ActionBody::not_found(), "static", 0),
                    }
                }

                // Opening is the existence check. A directory must count as
                // a miss: open() succeeds on one and would otherwise fail
                // mid-stream, after the headers promised a body.
                //
                // open and stat share one `spawn_blocking`; separate
                // `tokio::fs` calls would each be their own trip through the
                // blocking pool.
                let stat_result = match open_cached(&candidate) {
                    Some(opened) => opened.and_then(stat_and_advise),
                    None => {
                        let candidate = candidate.clone();
                        tokio::task::spawn_blocking(move || {
                            std::fs::File::open(&candidate).and_then(stat_and_advise)
                        })
                        .await
                        .expect("blocking task panicked")
                    }
                };
                let (opened, kind) = match stat_result {
                    Ok((_file, meta)) if meta.is_dir() => (None, FsKind::Dir),
                    Ok((file, meta)) => (Some((file, meta)), FsKind::File),
                    Err(_) => (None, FsKind::Missing),
                };
                state.fs_cache.put(candidate.clone(), kind);
                match opened {
                    Some((file, meta)) => {
                        return DispatchResult::new(
                            ActionBody::StaticFile {
                                file,
                                meta,
                                candidate,
                            },
                            "static",
                            0,
                        );
                    }
                    None => match fallback {
                        Some(next) => {
                            current = next;
                            continue;
                        }
                        None => return DispatchResult::new(ActionBody::not_found(), "static", 0),
                    },
                }
            }
            RouteActionConfig::Return { status } => {
                let status = StatusCode::from_u16(*status).unwrap_or_else(|_| {
                    panic!("route return status {status} is not a valid HTTP status - config::validate should have caught this at startup")
                });
                return DispatchResult::new(
                    ActionBody::Buffered {
                        status,
                        body: Vec::new(),
                        headers: HeaderBlob::default(),
                    },
                    "return",
                    0,
                );
            }
            RouteActionConfig::Php { target } => {
                let mut result = match resolve_script(state, target, path).await {
                    Some(resolved) => match build_php_request(
                        state,
                        req,
                        path,
                        resolved,
                        ctx,
                        state.config.max_body_size,
                    )
                    .await
                    {
                        Ok((php_request, body_cleanup)) => {
                            dispatch_php(state, php_request, body_cleanup).await
                        }
                        Err(early_response) => *early_response,
                    },
                    None => DispatchResult::new(ActionBody::not_found(), "php-no-script", 0),
                };
                // Every outcome from here belongs to this one target.
                result.php_target = Some(Arc::clone(target));
                return result;
            }
        }
    }
}

/// Separate from `PhpRequest` so a miss 404s before the body is buffered.
pub(crate) struct ResolvedScript {
    pub(crate) script_path: String,
    pub(crate) document_root: String,
    pub(crate) script_name: String,
    pub(crate) path_info: String,
}

/// One fixed script for every request, with the whole path as PATH_INFO.
fn resolve_script_mode(root: &str, script: &str, url_path: &str) -> ResolvedScript {
    ResolvedScript {
        script_path: format!("{root}/{script}"),
        document_root: root.to_string(),
        script_name: format!("/{script}"),
        path_info: url_path.to_string(),
    }
}

pub(crate) fn kind_of(meta: std::io::Result<std::fs::Metadata>) -> FsKind {
    match meta {
        Ok(m) if m.is_dir() => FsKind::Dir,
        Ok(_) => FsKind::File,
        Err(_) => FsKind::Missing,
    }
}

/// Cached `metadata()`; only which file to hand the worker matters here.
///
/// A warm dentry is answered inline, but `open` demands read permission
/// where `stat` does not. Only a success or a genuine absence is therefore
/// conclusive; anything else must still ask the real `stat`.
pub(crate) async fn stat_kind(fs_cache: &FsCache, path: &std::path::Path) -> FsKind {
    if let Some(kind) = fs_cache.get(path) {
        return kind;
    }
    let kind = match open_cached(path) {
        Some(Ok(file)) => kind_of(file.metadata()),
        Some(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => FsKind::Missing,
        _ => kind_of(tokio::fs::metadata(path).await),
    };
    fs_cache.put(path.to_path_buf(), kind);
    kind
}

/// URL maps to a `.php` under `root`, with `index` appended for
/// directory-style requests and trailing segments becoming PATH_INFO. Serves
/// `index` directly rather than redirecting a directory to its trailing
/// slash.
async fn resolve_index_target(
    fs_cache: &FsCache,
    root: &str,
    url_path: &str,
    index: &str,
    allowed: &[String],
) -> Option<ResolvedScript> {
    let root_path = std::path::Path::new(root);
    let rel = url_path.trim_start_matches('/');
    let absolute = |rel_prefix: &str| format!("/{rel_prefix}");

    if url_path.ends_with('/') || rel.is_empty() {
        let candidate_rel = format!("{rel}{index}");
        let candidate = root_path.join(&candidate_rel);
        return (stat_kind(fs_cache, &candidate).await == FsKind::File).then(|| ResolvedScript {
            script_path: candidate.to_string_lossy().into_owned(),
            document_root: root.to_string(),
            script_name: absolute(&candidate_rel),
            path_info: String::new(),
        });
    }

    // One stat answers both questions.
    let direct = root_path.join(rel);
    match stat_kind(fs_cache, &direct).await {
        FsKind::File => {
            return Some(ResolvedScript {
                script_path: direct.to_string_lossy().into_owned(),
                document_root: root.to_string(),
                script_name: absolute(rel),
                path_info: String::new(),
            });
        }
        FsKind::Dir => {
            let candidate_rel = format!("{}/{index}", rel.trim_end_matches('/'));
            let candidate = root_path.join(&candidate_rel);
            return (stat_kind(fs_cache, &candidate).await == FsKind::File).then(|| {
                ResolvedScript {
                    script_path: candidate.to_string_lossy().into_owned(),
                    document_root: root.to_string(),
                    script_name: absolute(&candidate_rel),
                    path_info: String::new(),
                }
            });
        }
        FsKind::Missing => {}
    }

    // Longest executable-suffixed prefix wins.
    let segments: Vec<&str> = rel.split('/').collect();
    for cut in (1..segments.len()).rev() {
        // Suffix check first, before building a join for a cut that cannot
        // match.
        if !extension_is_listed(segments[cut - 1], allowed) {
            continue;
        }
        let prefix = segments[..cut].join("/");
        let candidate = root_path.join(&prefix);
        if stat_kind(fs_cache, &candidate).await == FsKind::File {
            return Some(ResolvedScript {
                script_path: candidate.to_string_lossy().into_owned(),
                document_root: root.to_string(),
                script_name: absolute(&prefix),
                path_info: format!("/{}", segments[cut..].join("/")),
            });
        }
    }
    None
}

/// An unknown target means `config::validate` was bypassed: a bug in that
/// check, not a bad request.
async fn resolve_script(state: &AppState, name: &str, url_path: &str) -> Option<ResolvedScript> {
    let target = state.config.php.targets.get(name).unwrap_or_else(|| {
        panic!("route referenced php target {name:?}, missing from php.targets - config::validate should have caught this at startup")
    });
    let allowed = &state.config.php.script_extensions;
    let resolved = match &target.script {
        Some(script) => Some(resolve_script_mode(&target.root, script, url_path)),
        None => {
            let index = target.index.as_deref().unwrap_or("index.php");
            resolve_index_target(&state.fs_cache, &target.root, url_path, index, allowed).await
        }
    };
    // Past every branch on purpose: a branch resolving a client-named file
    // cannot forget a check it does not perform itself.
    resolved.filter(|r| extension_is_listed(&r.script_path, allowed))
}
