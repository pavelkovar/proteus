use super::*;

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("proteus-openat2-{}-{name}", std::process::id()))
}

/// A file just written has a warm dentry, which is the whole point: the open
/// is answered inline and the blocking pool is never involved.
#[test]
fn open_cached_answers_a_warm_dentry_inline() {
    let path = temp_path("warm");
    std::fs::write(&path, b"x").unwrap();
    let result = open_cached(&path);
    let _ = std::fs::remove_file(&path);
    match result {
        Some(Ok(_)) => {}
        Some(Err(e)) => panic!("a readable file must open, got {e}"),
        // Only legitimate when the kernel has no RESOLVE_CACHED at all.
        None => assert!(!openat2_usable_for_test(), "a warm dentry must not defer"),
    }
}

/// The errno distinction that keeps a miss from being opened twice: a
/// genuinely absent file is an answer, not a reason to consult the pool.
#[test]
fn open_cached_reports_a_missing_file_rather_than_deferring_it() {
    let path = temp_path("missing");
    let _ = std::fs::remove_file(&path);
    match open_cached(&path) {
        Some(Err(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
        Some(Ok(_)) => panic!("a file that does not exist must not open"),
        None => assert!(
            !openat2_usable_for_test(),
            "ENOENT is the real answer and must not be deferred to the blocking pool"
        ),
    }
}

/// `open()` succeeds on a directory, so the caller - not this helper - is
/// what keeps one from being streamed as a body.
#[test]
fn open_cached_opens_a_directory_which_stat_then_classifies() {
    let dir = temp_path("dir");
    std::fs::create_dir_all(&dir).unwrap();
    let result = open_cached(&dir);
    let _ = std::fs::remove_dir(&dir);
    if let Some(Ok(file)) = result {
        let (_, meta) = stat_and_advise(file).unwrap();
        assert!(meta.is_dir());
    }
}

/// The inline path and the `stat` fallback must classify identically, or
/// which one answered would change what the worker is handed.
#[tokio::test]
async fn stat_kind_classifies_the_same_however_it_was_answered() {
    let cache = FsCache::new(64, std::time::Duration::from_secs(60));
    let dir = temp_path("statkind");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("script.php");
    std::fs::write(&file, b"<?php").unwrap();
    let absent = dir.join("nope.php");

    assert_eq!(stat_kind(&cache, &file).await, FsKind::File);
    assert_eq!(stat_kind(&cache, &dir).await, FsKind::Dir);
    assert_eq!(stat_kind(&cache, &absent).await, FsKind::Missing);

    // Same verdicts with the fast path out of the picture.
    let cold = FsCache::new(64, std::time::Duration::from_secs(60));
    assert_eq!(kind_of(std::fs::metadata(&file)), FsKind::File);
    assert_eq!(kind_of(std::fs::metadata(&dir)), FsKind::Dir);
    assert_eq!(kind_of(std::fs::metadata(&absent)), FsKind::Missing);
    assert_eq!(stat_kind(&cold, &file).await, FsKind::File);

    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rejects_parent_dir_traversal() {
    // Live-exploitable before this check existed.
    assert!(path_escapes_root("/../../../../etc/passwd"));
    assert!(path_escapes_root("/static/../../etc/passwd"));
    assert!(path_escapes_root("/a/../../b"));
    assert!(!path_escapes_root("/static/logo.png"));
    assert!(!path_escapes_root("/"));
    // Not a traversal component, and must not be rejected.
    assert!(!path_escapes_root("/file..name.txt"));
}

#[test]
fn percent_decode_leaves_an_unencoded_path_borrowed() {
    let decoded = percent_decode_path("/a/b/c.txt").unwrap();
    assert!(
        matches!(decoded, std::borrow::Cow::Borrowed(_)),
        "the common case must not allocate"
    );
    assert_eq!(decoded, "/a/b/c.txt");
}

#[test]
fn percent_decode_handles_spaces_and_multibyte() {
    assert_eq!(
        percent_decode_path("/my%20file.txt").unwrap(),
        "/my file.txt"
    );
    // One codepoint spread over two escapes: validating per-escape rejects it.
    assert_eq!(percent_decode_path("/%C3%A1%C4%8D.php").unwrap(), "/áč.php");
    assert_eq!(percent_decode_path("/a%2Bb%3Dc").unwrap(), "/a+b=c");
    assert_eq!(percent_decode_path("/%2e").unwrap(), "/.");
}

/// Why decoding must precede the traversal check: the raw path hides these.
#[test]
fn decoded_traversal_is_caught_by_the_traversal_check() {
    for raw in [
        "/%2e%2e/etc/passwd",
        "/a/%2E%2E/%2E%2E/etc/passwd",
        "/%2e%2e",
    ] {
        let decoded = percent_decode_path(raw).expect("these decode fine, they're just hostile");
        assert!(
            path_escapes_root(&decoded),
            "{raw} decodes to {decoded:?}, which must be rejected as traversal"
        );
        assert!(
            !path_escapes_root(raw),
            "sanity: {raw} is exactly the shape the raw check misses"
        );
    }
}

/// An encoded separator invents path segments after routing and the
/// traversal check have already run on a different shape.
#[test]
fn percent_decode_rejects_encoded_separators() {
    for raw in ["/a%2Fb", "/a%2fb", "/%2e%2e%2fetc/passwd", "/a%5Cb"] {
        assert_eq!(
            percent_decode_path(raw),
            Err(PathDecodeError::EncodedSeparator),
            "{raw} must be refused"
        );
    }
}

#[test]
fn percent_decode_rejects_nul_malformed_and_non_utf8() {
    assert_eq!(percent_decode_path("/a%00b"), Err(PathDecodeError::Nul));
    assert_eq!(
        percent_decode_path("/a%zzb"),
        Err(PathDecodeError::Malformed)
    );
    assert_eq!(percent_decode_path("/a%2"), Err(PathDecodeError::Malformed));
    assert_eq!(percent_decode_path("/a%"), Err(PathDecodeError::Malformed));
    // Valid percent-encoding, invalid UTF-8.
    assert_eq!(
        percent_decode_path("/%FF%FE"),
        Err(PathDecodeError::NotUtf8)
    );
}
