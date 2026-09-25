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

#[test]
fn request_path_folds_empty_and_dot_segments() {
    for (raw, want) in [
        ("//admin/x", "/admin/x"),
        ("/./admin/x", "/admin/x"),
        ("/%2e/admin/x", "/admin/x"),
        ("/a//b///c", "/a/b/c"),
        ("/a/./b/.", "/a/b/"),
        ("/dir/", "/dir/"),
        ("/dir//", "/dir/"),
        ("/.", "/"),
        ("//", "/"),
        ("/", "/"),
        ("/.well-known/x", "/.well-known/x"),
        ("/a/.hidden", "/a/.hidden"),
        ("/a..b/c.", "/a..b/c."),
    ] {
        assert_eq!(request_path(raw).unwrap(), want, "for {raw:?}");
    }
}

#[test]
fn request_path_leaves_parent_segments_for_the_traversal_check() {
    for raw in ["/a/../b", "//../etc/passwd", "/./%2e%2e/etc/passwd"] {
        let path = request_path(raw).unwrap();
        assert!(
            path_escapes_root(&path),
            "{raw:?} became {path:?}, which no longer trips the traversal check"
        );
    }
}

#[test]
fn request_path_borrows_an_already_canonical_path() {
    for raw in ["/", "/index.php", "/a/b/c/", "/.well-known/acme"] {
        assert!(
            matches!(request_path(raw).unwrap(), std::borrow::Cow::Borrowed(_)),
            "{raw:?} was copied"
        );
    }
}

/// `cargo test --release --bin proteus -- --ignored bench_ --nocapture --test-threads=1`
#[test]
#[ignore]
fn bench_request_path_then_route() {
    use crate::config::{Route, RouteMatch};
    use crate::utils::match_pattern::MatchPattern;
    let route = |pat: &str, status: u16| Route {
        when: None,
        matcher: RouteMatch {
            uri: vec![MatchPattern::try_from(pat.to_string()).unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Return { status },
    };
    let mut cfg =
        crate::config::parse(r#"{ "listen": "127.0.0.1:0", "php": { "processes": {} } }"#).unwrap();
    cfg.routes = vec![
        route("/health", 200),
        route("/admin/*", 403),
        route("*.php", 404),
        route("/static/*", 200),
        route("*", 200),
    ];
    for (label, paths) in [
        (
            "canonical paths",
            vec![
                "/",
                "/products/123",
                "/static/app.3f2a1b.css",
                "/api/v1/orders?x=1",
                "/a/b/c/d/e/f/g/h/index.html",
            ],
        ),
        (
            "paths needing work",
            vec![
                "//admin/x",
                "/./a/b",
                "/a//b///c/",
                "/%2e/static/x.css",
                "/p/./q/./r/.",
            ],
        ),
        (
            "percent-encoded",
            vec!["/search/caf%C3%A9", "/files/a%20b%20c.txt", "/x/%7Euser/y"],
        ),
    ] {
        let iters = 2_000_000usize;
        let mut hits = 0usize;
        for round in 0..3 {
            let t = std::time::Instant::now();
            for i in 0..iters {
                let raw = paths[i % paths.len()];
                let path = request_path(raw).unwrap();
                if matches!(
                    match_route(&cfg, &path, "GET", ""),
                    RouteDecision::Matched { .. }
                ) {
                    hits += 1;
                }
            }
            if round == 2 {
                println!(
                    "BENCH request_path + match_route, {label:20} {:6.1} ns/request",
                    t.elapsed().as_nanos() as f64 / iters as f64
                );
            }
        }
        std::hint::black_box(hits);
    }
}

#[test]
fn only_filesystems_known_to_answer_open_from_memory_count_as_local() {
    for local in [0xEF53, 0x5846_5342, 0x9123_683E, 0x0102_1994, 0x794C_7630] {
        assert!(is_local_fs_type(local), "{local:#x} should be local");
    }
    // NFS, FUSE, CIFS, SMB2, Ceph, procfs.
    for remote in [
        0x6969,
        0x6573_5546,
        0xFF53_4D42,
        0xFE53_4D42,
        0x00C3_6400,
        0x9FA0,
    ] {
        assert!(!is_local_fs_type(remote), "{remote:#x} must not be local");
    }
}

#[test]
fn on_local_fs_reads_the_filesystem_an_fd_is_on() {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("proteus-local-fs-{}", std::process::id()));
    std::fs::write(&path, b"x").unwrap();
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    let c = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::statfs(c.as_ptr(), &mut st) }, 0);
    assert_eq!(
        on_local_fs(&std::fs::File::open(&path).unwrap()),
        is_local_fs_type(st.f_type as u64 as u32)
    );
    std::fs::remove_file(&path).unwrap();
    assert!(!on_local_fs(
        &std::fs::File::open("/proc/self/status").unwrap()
    ));
}
