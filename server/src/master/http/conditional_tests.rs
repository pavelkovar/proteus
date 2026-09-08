use super::*;
use std::time::Duration;

fn write_temp_file(name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("conditional-test-{name}-{}", std::process::id()));
    std::fs::write(&path, content).unwrap();
    path
}

fn etag(s: &str) -> headers::ETag {
    s.parse().unwrap()
}

/// For the cases the crate's own constructors do not cover, such as a real
/// comma-separated `If-None-Match` list.
fn typed<H: headers::Header>(value: &str) -> H {
    let hv = hyper::header::HeaderValue::from_str(value).unwrap();
    H::decode(&mut std::iter::once(&hv)).unwrap()
}

#[test]
fn make_etag_is_strong() {
    let path = write_temp_file("etag", b"hello world");
    let meta = std::fs::metadata(&path).unwrap();
    let etag = make_etag(meta.modified().ok(), meta.len()).expect("modified() is supported on Linux");
    assert!(!etag.starts_with("W/"), "must be a strong etag (If-Range needs strong comparison): {etag}");
    assert!(etag.starts_with('"') && etag.ends_with('"'), "must still be a quoted entity-tag: {etag}");
    std::fs::remove_file(&path).ok();
}

#[test]
fn make_etag_distinguishes_same_second_different_nanosecond() {
    // Two writes within one wall-clock second at the same length must still
    // differ, or a stale If-Range validates against new content. Constructed
    // times, so this does not depend on winning a timing race.
    let same_second = SystemTime::UNIX_EPOCH + Duration::new(1_700_000_000, 0);
    let a = make_etag(Some(same_second + Duration::from_nanos(123)), 100).unwrap();
    let b = make_etag(Some(same_second + Duration::from_nanos(456)), 100).unwrap();
    assert_ne!(a, b, "same second, same length, different nanosecond mtime must still change the ETag");
}

#[test]
fn weaken_etag_adds_the_weak_prefix_without_changing_the_opaque_tag() {
    let strong = make_etag(Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)), 100).unwrap();
    let weak = weaken_etag(&strong);
    assert_eq!(weak, format!("W/{strong}"));
}

#[test]
fn not_modified_matches_on_if_none_match() {
    let cond = ConditionalHeaders { if_none_match: Some(typed("W/\"abc-1\"")), ..Default::default() };
    assert!(not_modified(&cond, Some("W/\"abc-1\""), None));
    assert!(!not_modified(&cond, Some("W/\"different\""), None), "a different etag must not be treated as unmodified");
}

#[test]
fn not_modified_matches_any_tag_in_a_comma_separated_list() {
    // A real list, not a single value: an exact-string check cannot match it.
    let cond = ConditionalHeaders { if_none_match: Some(typed("\"a\", \"b\", \"c\"")), ..Default::default() };
    assert!(not_modified(&cond, Some("\"b\""), None));
    assert!(!not_modified(&cond, Some("\"z\""), None));
}

#[test]
fn not_modified_wildcard_if_none_match_always_matches() {
    let cond = ConditionalHeaders { if_none_match: Some(headers::IfNoneMatch::any()), ..Default::default() };
    assert!(not_modified(&cond, Some("W/\"whatever\""), None));
}

#[test]
fn not_modified_returns_false_when_if_none_match_is_set_but_no_etag_is_available() {
    // Nothing to compare against must not become a spurious 304.
    let cond = ConditionalHeaders { if_none_match: Some(typed("\"x\"")), ..Default::default() };
    assert!(!not_modified(&cond, None, None));
}

#[test]
fn not_modified_falls_back_to_if_modified_since_when_if_none_match_absent() {
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let same = headers::IfModifiedSince::from(modified);
    let later = headers::IfModifiedSince::from(modified + Duration::from_secs(60));
    let earlier = headers::IfModifiedSince::from(modified - Duration::from_secs(60));

    assert!(not_modified(&ConditionalHeaders { if_modified_since: Some(same), ..Default::default() }, Some("\"unrelated\""), Some(modified)));
    assert!(
        not_modified(&ConditionalHeaders { if_modified_since: Some(later), ..Default::default() }, Some("\"unrelated\""), Some(modified)),
        "client's cached copy is newer than or equal to the file - still not modified"
    );
    assert!(!not_modified(&ConditionalHeaders { if_modified_since: Some(earlier), ..Default::default() }, Some("\"unrelated\""), Some(modified)));
}

#[test]
fn not_modified_if_none_match_wins_over_if_modified_since() {
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let earlier = headers::IfModifiedSince::from(modified - Duration::from_secs(60));
    // If-Modified-Since alone would say modified; If-None-Match takes
    // precedence.
    let cond = ConditionalHeaders { if_none_match: Some(typed("\"tag\"")), if_modified_since: Some(earlier), ..Default::default() };
    assert!(not_modified(&cond, Some("\"tag\""), Some(modified)));
}

#[test]
fn if_range_absent_always_honors_range() {
    assert!(if_range_matches(&ConditionalHeaders::default(), Some("W/\"x\""), None));
    assert!(if_range_matches(&ConditionalHeaders::default(), None, None));
}

#[test]
fn if_range_matching_strong_etag_honors_range() {
    let cond = ConditionalHeaders { if_range: Some(headers::IfRange::etag(etag("\"x\""))), ..Default::default() };
    assert!(if_range_matches(&cond, Some("\"x\""), None));
}

#[test]
fn if_range_stale_etag_falls_back_to_full_response() {
    let cond = ConditionalHeaders { if_range: Some(headers::IfRange::etag(etag("\"old\""))), ..Default::default() };
    assert!(!if_range_matches(&cond, Some("\"new\""), None));
}

#[test]
fn if_range_rejects_a_weak_client_value_even_when_it_textually_matches() {
    // If-Range requires strong comparison (RFC 9110 §13.1.5), so a weak
    // value can never satisfy it even with an identical opaque tag.
    let cond = ConditionalHeaders { if_range: Some(headers::IfRange::etag(etag("W/\"x\""))), ..Default::default() };
    assert!(!if_range_matches(&cond, Some("\"x\""), None));
}

#[test]
fn if_range_matching_date_honors_range() {
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let cond = ConditionalHeaders { if_range: Some(headers::IfRange::date(modified)), ..Default::default() };
    assert!(if_range_matches(&cond, Some("W/\"unrelated\""), Some(modified)));
}
