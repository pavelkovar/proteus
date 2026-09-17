//! Conditional GET, the `If-Range` precondition, and the ETags attached to
//! static-file responses.

use headers::HeaderMapExt as _;
use std::time::{SystemTime, UNIX_EPOCH};

/// Extracted once, before the request is consumed. Typed values rather than
/// raw strings, since the `headers` crate already gets comma-separated
/// `If-None-Match` and weak/strong comparison right (RFC 9110 §8.8.3.2/§13.1.5).
#[derive(Default)]
pub(crate) struct ConditionalHeaders {
    pub(crate) if_none_match: Option<headers::IfNoneMatch>,
    pub(crate) if_modified_since: Option<headers::IfModifiedSince>,
    pub(crate) range: Option<String>,
    pub(crate) if_range: Option<headers::IfRange>,
}

impl ConditionalHeaders {
    pub(crate) fn from_headers(header_map: &hyper::HeaderMap) -> Self {
        ConditionalHeaders {
            if_none_match: header_map.typed_get(),
            if_modified_since: header_map.typed_get(),
            range: header_map
                .get(hyper::header::RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            if_range: header_map.typed_get(),
        }
    }
}

/// From metadata alone: no content hashing, no I/O beyond the `stat` already
/// done. Nanosecond precision, since RFC 9110 §13.1.5's strong comparison for
/// `If-Range` needs it: whole seconds could splice two file versions.
pub(crate) fn make_etag(modified: Option<SystemTime>, len: u64) -> Option<String> {
    let modified = modified?.duration_since(UNIX_EPOCH).ok()?;
    Some(format!(
        "\"{:x}-{:x}-{:x}\"",
        modified.as_secs(),
        modified.subsec_nanos(),
        len
    ))
}

/// Weak enough to fail `If-Range`'s strong comparison, so a resumed
/// download can't splice two representations together.
pub(crate) fn weaken_etag(etag: &str) -> String {
    format!("W/{etag}")
}

/// `If-None-Match` wins over `If-Modified-Since` when both are present
/// (RFC 9110 §13.1.1/§13.1.3).
///
/// Takes `etag` as a string and parses it here, so the parse happens only
/// when there is actually an `If-None-Match` to compare against.
pub(crate) fn not_modified(
    cond: &ConditionalHeaders,
    etag: Option<&str>,
    modified: Option<SystemTime>,
) -> bool {
    if let Some(inm) = &cond.if_none_match {
        return etag
            .and_then(|e| e.parse::<headers::ETag>().ok())
            .is_some_and(|etag| !inm.precondition_passes(&etag));
    }
    let (Some(ims), Some(modified)) = (&cond.if_modified_since, modified) else {
        return false;
    };
    !ims.is_modified(modified)
}

/// Whether to honour `Range` at all (RFC 9110 §13.1.5): absent always
/// honours it, present only while it still matches the current
/// representation, since the client's range basis may be stale.
pub(crate) fn if_range_matches(
    cond: &ConditionalHeaders,
    etag: Option<&str>,
    modified: Option<SystemTime>,
) -> bool {
    let Some(if_range) = &cond.if_range else {
        return true;
    };
    let etag = etag.and_then(|e| e.parse::<headers::ETag>().ok());
    let last_modified = modified.map(headers::LastModified::from);
    !if_range.is_modified(etag.as_ref(), last_modified.as_ref())
}

pub(crate) fn with_cache_headers(
    mut builder: hyper::http::response::Builder,
    etag: Option<&str>,
    modified: Option<SystemTime>,
) -> hyper::http::response::Builder {
    if let Some(etag) = etag {
        builder = builder.header(hyper::header::ETAG, etag);
    }
    if let Some(modified) = modified {
        builder = builder.header(
            hyper::header::LAST_MODIFIED,
            httpdate::fmt_http_date(modified),
        );
    }
    builder
}

#[cfg(test)]
#[path = "conditional_tests.rs"]
mod tests;
