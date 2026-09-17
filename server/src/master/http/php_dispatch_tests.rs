use super::*;

#[test]
fn method_cow_borrows_every_standard_method() {
    for m in [
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::HEAD,
        Method::OPTIONS,
        Method::CONNECT,
        Method::PATCH,
        Method::TRACE,
    ] {
        assert!(
            matches!(method_cow(&m), Cow::Borrowed(_)),
            "{m} must not allocate"
        );
    }
}

#[test]
fn method_cow_allocates_for_a_real_extension_method() {
    let custom = Method::from_bytes(b"PURGE").unwrap();
    assert!(matches!(method_cow(&custom), Cow::Owned(s) if s == "PURGE"));
}

#[test]
fn version_str_maps_every_known_version() {
    assert_eq!(version_str(Version::HTTP_09), "HTTP/0.9");
    assert_eq!(version_str(Version::HTTP_10), "HTTP/1.0");
    assert_eq!(version_str(Version::HTTP_11), "HTTP/1.1");
    assert_eq!(version_str(Version::HTTP_2), "HTTP/2.0");
    assert_eq!(version_str(Version::HTTP_3), "HTTP/3.0");
}
