//! Assembling a `Response<ResponseBody>`: buffered, streamed static file,
//! or streamed PHP output.

use super::compression::{
    body_from_stream, compressed_body, compression_eligible, pick_encoding_when_eligible, stream_size_gate,
    with_content_encoding,
};
use super::conditional::{if_range_matches, make_etag, not_modified, weaken_etag, with_cache_headers, ConditionalHeaders};
use super::range::{parse_range, FileBody};
use super::ResponseBody;
use crate::ipc::data::HeaderBlob;
use crate::master::pool_manager::BodyStream;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Response, StatusCode};

struct ScriptHeaders<'a> {
    content_type: &'a str,
    declared_len: Option<usize>,
    /// The script encoded the body itself, so its bytes and its
    /// `Content-Encoding` belong together and neither may be touched.
    pre_encoded: bool,
}

/// The header facts compression needs, in one walk rather than a `get` per
/// name. Runs to the end rather than stopping once the first two are found:
/// an absent `Content-Encoding` is only provable by looking at every header.
fn script_headers<'b>(headers: &'b HeaderBlob<'_>) -> ScriptHeaders<'b> {
    let mut out = ScriptHeaders { content_type: "", declared_len: None, pre_encoded: false };
    for (name, value) in headers.iter() {
        if out.content_type.is_empty() && name.eq_ignore_ascii_case("content-type") {
            out.content_type = value;
        } else if out.declared_len.is_none() && name.eq_ignore_ascii_case("content-length") {
            out.declared_len = value.trim().parse().ok();
        } else if name.eq_ignore_ascii_case("content-encoding") {
            out.pre_encoded = true;
        }
    }
    out
}

/// Extension-based, not content-sniffing.
fn guess_mime_type(path: &str) -> mime_guess::Mime {
    mime_guess::from_path(path).first_or_octet_stream()
}

/// The `Infallible` error is mapped only to unify with `streamed_body`.
fn buffered_body(bytes: Vec<u8>) -> ResponseBody {
    Full::new(Bytes::from(bytes)).map_err(|never: std::convert::Infallible| match never {}).boxed()
}

/// Message-framing headers are master's alone to set. A script-set
/// Content-Length that disagrees with the real body does not merely corrupt
/// this response - it desyncs whatever the client reads next off a reused
/// connection.
fn is_framing_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("transfer-encoding") || name.eq_ignore_ascii_case("connection")
}

/// Appends rather than inserting, so repeated names such as `Set-Cookie`
/// all survive.
///
/// A script can put arbitrary bytes into a header via `header()`, which only
/// rejects embedded CR/LF - not, say, other control characters that are
/// still invalid HTTP grammar. `builder.header()` would silently poison every
/// header queued after a bad one until `.body()` surfaces one accumulated
/// error, so a name/value that cannot become valid HTTP is dropped here
/// instead, individually, before it ever reaches the builder.
fn apply_headers(mut builder: hyper::http::response::Builder, headers: &HeaderBlob<'_>) -> hyper::http::response::Builder {
    for (name, value) in headers.iter() {
        if is_framing_header(name) {
            continue;
        }
        if hyper::header::HeaderName::from_bytes(name.as_bytes()).is_err()
            || hyper::header::HeaderValue::from_bytes(value.as_bytes()).is_err()
        {
            tracing::warn!(r#type = "controller", header = name, "dropping a response header that is not valid HTTP");
            continue;
        }
        builder = builder.header(name, value);
    }
    builder
}

/// Does not negotiate `Accept-Encoding`: every caller passes a small fixed
/// string, never worth compressing.
pub(crate) fn build_response(status: StatusCode, body: Vec<u8>, headers: &HeaderBlob<'_>) -> Response<ResponseBody> {
    apply_headers(Response::builder().status(status), headers).body(buffered_body(body)).unwrap()
}

/// `meta` must come from the same `stat` that opened `file`; an fd stays
/// valid regardless of what happens to the path afterwards.
///
/// The encoding is decided up front so a 304's ETag matches this
/// negotiation. `Range` always serves identity bytes, a range being
/// meaningless against anything but a fixed representation, so a compressed
/// 200 never advertises `Accept-Ranges`.
///
/// A HEAD sends GET's headers but must not build the body: `compressed_body`
/// starts its blocking-pool task the instant it is called, and hyper would
/// discard the result unread.
pub(crate) async fn build_static_response(
    file: std::fs::File,
    meta: &std::fs::Metadata,
    candidate: &std::path::Path,
    path: &str,
    accept_encoding: &str,
    min_size_bytes: usize,
    mime_types: &[String],
    cond: &ConditionalHeaders,
    is_head: bool,
) -> Response<ResponseBody> {
    let len = meta.len();
    let mime = guess_mime_type(candidate.to_str().unwrap_or(path));
    let content_type = mime.essence_str();
    let modified = meta.modified().ok();
    let identity_etag = make_etag(modified, len);
    // Before Accept-Encoding: Vary depends on whether the response could
    // ever vary by encoding, not on what this client happens to accept.
    let vary = compression_eligible(len as usize, min_size_bytes, content_type, mime_types);
    // `vary` is the eligibility answer already computed.
    let encoding = pick_encoding_when_eligible(vary, accept_encoding);
    let negotiated_etag = if encoding.is_some() { identity_etag.as_deref().map(weaken_etag) } else { identity_etag.clone() };

    if not_modified(cond, negotiated_etag.as_deref(), modified) {
        let mut builder = with_cache_headers(Response::builder().status(StatusCode::NOT_MODIFIED), negotiated_etag.as_deref(), modified)
            .header(hyper::header::ACCEPT_RANGES, "bytes");
        if vary {
            builder = builder.header(hyper::header::VARY, "Accept-Encoding");
        }
        return builder.body(buffered_body(Vec::new())).unwrap();
    }

    if let Some(range) = &cond.range
        && if_range_matches(cond, identity_etag.as_deref(), modified)
    {
        match parse_range(range, len) {
            Some(Ok((start, end))) => {
                let range_len = end - start + 1;
                let builder = with_cache_headers(
                    Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(hyper::header::CONTENT_TYPE, content_type)
                        .header(hyper::header::CONTENT_LENGTH, range_len)
                        .header(hyper::header::CONTENT_RANGE, format!("bytes {start}-{end}/{len}"))
                        .header(hyper::header::ACCEPT_RANGES, "bytes"),
                    identity_etag.as_deref(),
                    modified,
                );
                return builder.body(body_from_stream(FileBody::new(file, start, range_len))).unwrap();
            }
            Some(Err(())) => {
                let builder = with_cache_headers(
                    Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header(hyper::header::CONTENT_RANGE, format!("bytes */{len}"))
                        .header(hyper::header::ACCEPT_RANGES, "bytes"),
                    identity_etag.as_deref(),
                    modified,
                );
                return builder.body(buffered_body(Vec::new())).unwrap();
            }
            None => {} // unparseable/multi-range - ignore, fall through to a normal 200
        }
    }

    let mut builder = with_cache_headers(
        Response::builder().status(StatusCode::OK).header(hyper::header::CONTENT_TYPE, content_type),
        negotiated_etag.as_deref(),
        modified,
    );
    if vary {
        builder = builder.header(hyper::header::VARY, "Accept-Encoding");
    }
    let body = match encoding {
        None => {
            builder = builder.header(hyper::header::CONTENT_LENGTH, len).header(hyper::header::ACCEPT_RANGES, "bytes");
            if is_head { buffered_body(Vec::new()) } else { body_from_stream(FileBody::new(file, 0, len)) }
        }
        Some(encoding) => {
            builder = with_content_encoding(builder, encoding);
            if is_head {
                buffered_body(Vec::new())
            } else {
                compressed_body(FileBody::new(file, 0, len), encoding, Some(len))
            }
        }
    };
    builder.body(body).unwrap()
}

/// The body may still be arriving, so eligibility is decided from
/// Content-Type alone and every frame is forwarded as it comes. This never
/// delays headers or early chunks, at the price of occasionally compressing
/// a response that turns out to be tiny.
pub(crate) fn build_php_stream_response(
    status: StatusCode,
    headers: &HeaderBlob<'_>,
    body: BodyStream,
    accept_encoding: &str,
    min_size_bytes: usize,
    mime_types: &[String],
) -> Response<ResponseBody> {
    // The Content-Length is a size hint only - see `stream_size_gate`.
    let script = script_headers(headers);
    let builder = apply_headers(Response::builder().status(status), headers);
    let (body_len, min_size) = stream_size_gate(script.declared_len, min_size_bytes);
    // A body the script already encoded must be left alone: encoding it again
    // yields two `Content-Encoding` headers and bytes no client can decode.
    let eligible = !script.pre_encoded && compression_eligible(body_len, min_size, script.content_type, mime_types);
    // Vary is about whether some Accept-Encoding could change this response,
    // not whether this client's did. A script that encoded the body did its
    // own negotiation, so that case varies too.
    let vary = eligible || script.pre_encoded;
    let builder = if vary { builder.header(hyper::header::VARY, "Accept-Encoding") } else { builder };

    match pick_encoding_when_eligible(eligible, accept_encoding) {
        None => builder.body(body_from_stream(body)).unwrap(),
        Some(encoding) => {
            with_content_encoding(builder, encoding).body(compressed_body(body, encoding, script.declared_len.map(|l| l as u64))).unwrap()
        }
    }
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
