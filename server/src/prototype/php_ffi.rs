//! The PHP module behind its C ABI. Only primitives and opaque pointers
//! cross this boundary.

use crate::ipc::data::{HeaderBlob, PhpRequest, RequestBody};
use libloading::os::unix::{Library, Symbol, RTLD_GLOBAL, RTLD_NOW};
use std::ffi::CString;
use std::io::Write;
use std::os::raw::{c_char, c_int, c_ulong, c_void};

/// The CGI extra-vars array in one allocation, each `KEY=VALUE\0` appended
/// directly rather than through a per-var `CString`.
struct CgiVarBuf {
    buf: Vec<u8>,
    offsets: Vec<usize>,
}

impl CgiVarBuf {
    fn with_capacity(byte_capacity: usize) -> Self {
        CgiVarBuf { buf: Vec::with_capacity(byte_capacity), offsets: Vec::new() }
    }

    /// Takes `fmt::Arguments` so a value needing formatting still writes
    /// straight into the buffer.
    fn push(&mut self, key: &str, value: std::fmt::Arguments<'_>) {
        let start = self.buf.len();
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(b'=');
        self.buf.write_fmt(value).expect("Vec<u8> writes are infallible");
        self.finish_entry(start);
    }

    /// Appends an HTTP header as its CGI `HTTP_`-prefixed var.
    fn push_header(&mut self, name: &str, value: &str) {
        let start = self.buf.len();
        write_header_cgi_key(&mut self.buf, name);
        self.buf.push(b'=');
        self.buf.extend_from_slice(value.as_bytes());
        self.finish_entry(start);
    }

    /// A NUL would truncate the C string and corrupt the following entry, so
    /// the whole var is dropped instead.
    fn finish_entry(&mut self, start: usize) {
        if self.buf[start..].contains(&0) {
            self.buf.truncate(start);
            return;
        }
        self.offsets.push(start);
        self.buf.push(0);
    }

    /// Invalidated by the next `push`, which may reallocate.
    fn pointers(&self) -> Vec<*const c_char> {
        self.offsets.iter().map(|&off| unsafe { self.buf.as_ptr().add(off).cast() }).collect()
    }
}

#[repr(C)]
struct CPhpRequest {
    method: *const c_char,
    uri: *const c_char,
    query_string: *const c_char,
    content_type: *const c_char, // may be null
    extra_vars: *const *const c_char,
    extra_var_count: c_ulong,
    body: *const c_char,           // null if body_file_path is set instead
    body_len: c_ulong,
    body_file_path: *const c_char, // null if body/body_len are used instead
    cookie_header: *const c_char, // may be null
    authorization: *const c_char, // may be null
}

/// Matches `proteus_php_mod_chunk_kind` in the C header.
const PROTEUS_PHP_MOD_CHUNK_HEADERS: c_int = 1;
const PROTEUS_PHP_MOD_CHUNK_BODY: c_int = 2;

/// One event in a response stream: `Headers` first, then any number of
/// `Body`, then exactly one `End`. Borrows C scratch buffers, so it is valid
/// only for the duration of the callback receiving it.
pub enum PhpChunk<'a> {
    Headers { status: u16, headers: HeaderBlob<'static> },
    Body(&'a [u8]),
    End,
}

/// Matches `proteus_php_mod_chunk_fn` in the C header. `data` is valid only
/// for the duration of the call.
type ChunkFn =
    unsafe extern "C" fn(c_int, c_int, *const c_char, c_ulong, *mut c_void);

type InitFn = unsafe extern "C" fn(
    *const *const c_char,
    c_ulong,
    *const *const c_char,
    c_ulong,
) -> c_int;
type ExecuteFileFn = unsafe extern "C" fn(
    *const c_char,
    *const CPhpRequest,
    Option<ChunkFn>,
    *mut c_void,
    *mut c_int,
) -> c_int;

pub struct PhpConn {
    init: Symbol<InitFn>,
    execute_file: Symbol<ExecuteFileFn>,
}

pub struct ExecuteResult {
    /// `fastcgi_finish_request()` fired `End` early, so the worker must
    /// still be treated as busy until `execute_file` itself returns.
    pub early_sent: bool,
}

impl PhpConn {
    /// Must use `RTLD_GLOBAL`: under the default `RTLD_LOCAL`, `libphp`'s
    /// symbols are hidden from PHP's own extension `dlopen()`s, which then
    /// fail with undefined symbols.
    pub fn load(path: &str) -> std::io::Result<Self> {
        let to_io_err = |e: libloading::Error| std::io::Error::other(e.to_string());
        let lib = unsafe { Library::open(Some(path), RTLD_NOW | RTLD_GLOBAL) }.map_err(to_io_err)?;
        // Deliberately leaked: held for the whole process lifetime, so there
        // is no earlier point at which unloading would be correct.
        let lib: &'static Library = Box::leak(Box::new(lib));
        unsafe {
            Ok(PhpConn {
                init: lib.get(b"proteus_php_mod_init").map_err(to_io_err)?,
                execute_file: lib.get(b"proteus_php_mod_execute_file").map_err(to_io_err)?,
            })
        }
    }

    /// Exactly once, in the prototype, before any `fork()`.
    pub fn init(&self, admin_entries: &[String], user_entries: &[String]) -> std::io::Result<()> {
        let to_cstrings = |entries: &[String]| -> Vec<CString> {
            entries
                .iter()
                .map(|e| CString::new(e.as_str()).unwrap_or_default())
                .collect()
        };
        let admin_cstrings = to_cstrings(admin_entries);
        let user_cstrings = to_cstrings(user_entries);
        let admin_ptrs: Vec<*const c_char> = admin_cstrings.iter().map(|s| s.as_ptr()).collect();
        let user_ptrs: Vec<*const c_char> = user_cstrings.iter().map(|s| s.as_ptr()).collect();

        let rc = unsafe {
            (self.init)(
                admin_ptrs.as_ptr(),
                admin_ptrs.len() as c_ulong,
                user_ptrs.as_ptr(),
                user_ptrs.len() as c_ulong,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("proteus_php_mod_init failed"))
        }
    }

    /// Runs one script, with its own request startup/shutdown pair, and is
    /// safe to call repeatedly. `on_chunk` fires synchronously and nothing is
    /// buffered.
    pub fn execute_file(&self, script_path: &str, req: &PhpRequest<'_>, on_chunk: &mut dyn FnMut(PhpChunk)) -> ExecuteResult {
        // Unreachable through hyper, but a panic on request-derived data
        // would take the whole worker down rather than failing one request.
        let Ok(c_path) = CString::new(script_path) else {
            on_chunk(PhpChunk::Headers { status: 500, headers: HeaderBlob::default() });
            on_chunk(PhpChunk::End);
            return ExecuteResult { early_sent: false };
        };
        let c_method = CString::new(req.method.as_ref()).unwrap_or_default();
        let c_uri = CString::new(req.uri.as_ref()).unwrap_or_default();
        let c_query = CString::new(req.query_string.as_ref()).unwrap_or_default();
        // PHP wants a NULL here, not an empty string.
        let c_content_type = (!req.content_type.is_empty())
            .then(|| CString::new(req.content_type.as_ref()).unwrap_or_default());

        // Not `req.uri`, which is path plus query; PHP_SELF must never carry
        // the query string.
        let document_root = req.document_root.as_ref();
        let script_name = req.script_name.as_ref();
        let path_info = req.path_info.as_ref();

        // Sized in one shot so the loop never reallocates. The CGI form
        // spends four bytes per header beyond what `byte_len` counts, hence
        // the extra term.
        let header_bytes = req.headers.byte_len() + 6 * req.headers.iter().count();
        let mut vars = CgiVarBuf::with_capacity(
            256 + document_root.len() + 2 * (script_name.len() + path_info.len()) + header_bytes,
        );
        vars.push("REMOTE_ADDR", format_args!("{}", req.client_ip));
        vars.push("SERVER_NAME", format_args!("{}", req.server_name));
        vars.push("SERVER_PORT", format_args!("{}", req.server_port));
        vars.push("SERVER_PROTOCOL", format_args!("{}", req.server_protocol));
        vars.push("DOCUMENT_ROOT", format_args!("{document_root}"));
        vars.push("SCRIPT_FILENAME", format_args!("{script_path}"));
        vars.push("SCRIPT_NAME", format_args!("{script_name}"));
        vars.push("PATH_INFO", format_args!("{path_info}"));
        vars.push("PHP_SELF", format_args!("{script_name}{path_info}"));
        if req.https {
            // Absent entirely over plain HTTP, per CGI convention, so that
            // `!empty($_SERVER['HTTPS'])` behaves as PHP code expects.
            vars.push("HTTPS", format_args!("on"));
        }
        // Captured in this same pass rather than two more scans.
        let mut cookie_value: Option<&str> = None;
        let mut authorization_value: Option<&str> = None;
        for (name, value) in req.headers.iter() {
            if suppressed_request_header(name) {
                continue;
            }
            if cookie_value.is_none() && name.eq_ignore_ascii_case("cookie") {
                cookie_value = Some(value);
            } else if authorization_value.is_none() && name.eq_ignore_ascii_case("authorization") {
                authorization_value = Some(value);
            }
            vars.push_header(name, value);
        }
        let extra_var_ptrs = vars.pointers();

        let c_cookie = cookie_value.map(|v| CString::new(v).unwrap_or_default());
        let c_authorization = authorization_value.map(|v| CString::new(v).unwrap_or_default());

        // Exactly one of the inline body and the body path is set.
        // `c_body_path` is bound here so it outlives `c_req`.
        let c_body_path;
        let (body_ptr, body_len, body_file_path_ptr) = match &req.body {
            RequestBody::Inline(bytes) => (bytes.as_ptr() as *const c_char, bytes.len() as c_ulong, std::ptr::null()),
            RequestBody::File { path, len } => {
                c_body_path = CString::new(path.as_ref()).unwrap_or_default();
                (std::ptr::null(), *len as c_ulong, c_body_path.as_ptr())
            }
        };

        let c_req = CPhpRequest {
            method: c_method.as_ptr(),
            uri: c_uri.as_ptr(),
            query_string: c_query.as_ptr(),
            content_type: c_content_type.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
            extra_vars: extra_var_ptrs.as_ptr(),
            extra_var_count: extra_var_ptrs.len() as c_ulong,
            body: body_ptr,
            body_len,
            body_file_path: body_file_path_ptr,
            cookie_header: c_cookie.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
            authorization: c_authorization.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
        };

        // C sees only the trampoline and an opaque pointer to a local
        // holding the real closure.
        let mut cb_ref: &mut dyn FnMut(PhpChunk) = &mut *on_chunk;
        let cb_user_data = &mut cb_ref as *mut _ as *mut c_void;

        let mut out_early_sent: c_int = 0;
        let rc = unsafe {
            (self.execute_file)(c_path.as_ptr(), &c_req, Some(chunk_trampoline), cb_user_data, &mut out_early_sent)
        };

        if rc != 0 {
            // request_startup() failed, so the callback never fired;
            // synthesize a response rather than emitting no chunks at all.
            on_chunk(PhpChunk::Headers { status: 500, headers: HeaderBlob::default() });
            on_chunk(PhpChunk::End);
            return ExecuteResult { early_sent: false };
        }

        ExecuteResult { early_sent: out_early_sent != 0 }
    }
}

/// Rebuilds the closure from `user_data` and calls it once. `data` lives
/// only for this call, and `PhpChunk::Body` inherits that borrow.
// Looks like a no-op on aarch64, where `c_char` is `u8`, but it is `i8` on
// most other targets.
#[allow(clippy::unnecessary_cast)]
unsafe extern "C" fn chunk_trampoline(
    kind: c_int,
    status: c_int,
    data: *const c_char,
    data_len: c_ulong,
    user_data: *mut c_void,
) {
    let cb = unsafe { &mut *(user_data as *mut &mut dyn FnMut(PhpChunk)) };
    let bytes: &[u8] = if data.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(data as *const u8, data_len as usize) }
    };
    if kind == PROTEUS_PHP_MOD_CHUNK_HEADERS {
        cb(PhpChunk::Headers { status: status.clamp(100, 599) as u16, headers: parse_headers(bytes) });
    } else if kind == PROTEUS_PHP_MOD_CHUNK_BODY {
        cb(PhpChunk::Body(bytes));
    } else {
        cb(PhpChunk::End);
    }
}

/// Request headers that must never become a `$_SERVER['HTTP_*']` var.
///
/// - Underscore: `X-Foo-Bar` and `X_Foo_Bar` map to the same CGI var with no
///   inverse (RFC 3875 §4.1.18), so dropping beats letting one spoof the other.
/// - Content-Type and Content-Length: already set as their own CGI vars.
/// - `Proxy`: httpoxy (CVE-2016-5385), where `HTTP_PROXY` is read as an
///   upstream proxy by several PHP HTTP clients. PHP has refused to register
///   it since 5.5.38, so this is a second, version-independent layer.
fn suppressed_request_header(name: &str) -> bool {
    name.contains('_')
        || name.eq_ignore_ascii_case("content-type")
        || name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("proxy")
}

/// Uppercased, hyphens to underscores, `HTTP_` prefix.
fn write_header_cgi_key(out: &mut Vec<u8>, name: &str) {
    out.extend_from_slice(b"HTTP_");
    let mut char_buf = [0u8; 4];
    for c in name.chars() {
        if c == '-' {
            out.push(b'_');
        } else {
            for upper in c.to_uppercase() {
                out.extend_from_slice(upper.encode_utf8(&mut char_buf).as_bytes());
            }
        }
    }
}

/// Splits the newline-joined header lines into a [`HeaderBlob`], in one
/// allocation for the whole set. `\n` is a safe separator because PHP's
/// `header()` has rejected embedded CR/LF since 5.1.2.
fn parse_headers(buf: &[u8]) -> HeaderBlob<'static> {
    if buf.is_empty() {
        return HeaderBlob::default();
    }
    let text = String::from_utf8_lossy(buf);
    // The blob trades ": " for two NULs per line, so the input length is
    // already a tight upper bound.
    let mut blob = HeaderBlob::with_capacity(buf.len() + 2);
    for line in text.split('\n') {
        if let Some((name, value)) = line.split_once(':') {
            blob.push(name.trim(), value.trim());
        }
    }
    blob
}

#[cfg(test)]
#[path = "php_ffi_tests.rs"]
mod tests;
