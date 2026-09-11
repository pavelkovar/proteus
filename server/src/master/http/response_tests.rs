use super::*;

fn blob(pairs: &[(&str, &str)]) -> HeaderBlob<'static> {
    let mut b = HeaderBlob::default();
    for (n, v) in pairs {
        b.push(n, v);
    }
    b
}

/// A `Content-Encoding` standing behind the other two must still be seen -
/// see `script_headers` for why the walk cannot stop early.
#[test]
fn script_headers_sees_a_content_encoding_behind_the_other_two() {
    let headers = blob(&[
        ("Content-Type", "text/plain"),
        ("Content-Length", "42"),
        ("X-Filler", "x"),
        ("Content-Encoding", "gzip"),
    ]);
    let found = script_headers(&headers);
    assert_eq!(found.content_type, "text/plain");
    assert_eq!(found.declared_len, Some(42));
    assert!(
        found.pre_encoded,
        "a Content-Encoding after the first two headers was missed"
    );
}

#[test]
fn script_headers_reports_no_encoding_when_the_script_set_none() {
    let headers = blob(&[("Content-Type", "text/html"), ("X-Other", "y")]);
    let found = script_headers(&headers);
    assert!(!found.pre_encoded);
    assert_eq!(found.declared_len, None);
}

/// A script can put a raw control byte into a header value (`header()` only
/// rejects CR/LF) without going through `hyper`'s own validation the way a
/// value parsed off the wire would have. Must be dropped individually, not
/// panic `build_response`'s `.unwrap()` - and a header queued after the bad
/// one must still survive, proving the whole builder was not poisoned by it.
#[test]
fn a_header_with_an_invalid_byte_is_dropped_rather_than_panicking() {
    let headers = blob(&[
        ("X-Good", "fine"),
        ("X-Reflected", "bad\x01value"),
        ("X-After", "also-fine"),
    ]);
    let resp = build_response(hyper::StatusCode::OK, Vec::new(), &headers);
    assert_eq!(resp.status(), hyper::StatusCode::OK);
    assert_eq!(resp.headers().get("x-good").unwrap(), "fine");
    assert!(
        resp.headers().get("x-reflected").is_none(),
        "the invalid header must be dropped, not crash the response"
    );
    assert_eq!(
        resp.headers().get("x-after").unwrap(),
        "also-fine",
        "a header queued after the bad one must still survive"
    );
}
