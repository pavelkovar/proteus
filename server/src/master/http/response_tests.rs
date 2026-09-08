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
    assert!(found.pre_encoded, "a Content-Encoding after the first two headers was missed");
}

#[test]
fn script_headers_reports_no_encoding_when_the_script_set_none() {
    let headers = blob(&[("Content-Type", "text/html"), ("X-Other", "y")]);
    let found = script_headers(&headers);
    assert!(!found.pre_encoded);
    assert_eq!(found.declared_len, None);
}
