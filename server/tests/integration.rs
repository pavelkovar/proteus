//! Integration tests: drive the real binary as an external process and
//! exercise it over HTTP.
//!
//! Must not call the pool or launch code in-process: it re-execs
//! `current_exe()` as `--internal-prototype`, which under `cargo test` is the
//! test binary, not the server.
//!
//! Needs the full PHP environment and a built php-mod, so these run only
//! inside this project's Docker dev environment.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct TestServer {
    child: Child,
    port: u16,
    status_port: u16,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Kill-on-drop for the one test that needs stdout piped, which `TestServer`
/// always discards.
struct ChildGuard<'a>(&'a mut Child);

impl Drop for ChildGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn php_mod_path() -> String {
    std::env::var("PROTEUS_PHP_MOD_PATH")
        .unwrap_or_else(|_| "/work/php-mod/libproteus-php-mod.so".to_string())
}

fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Blocks until the status endpoint answers, so the whole
/// prototype/privilege-drop chain is genuinely up.
async fn start_server(name: &str, php_root: &str, overrides: serde_json::Value) -> TestServer {
    let port = next_port();
    let status_port = next_port();

    let mut config = serde_json::json!({
        "listen": [format!("127.0.0.1:{port}")],
        "status": { "listen": format!("127.0.0.1:{status_port}") },
        "routes": [
            { "match": {}, "action": "static",
              "root": format!("{php_root}/public"),
              "fallback": { "action": "php", "target": "default" } }
        ],
        "php": {
            "targets": {
                "default": { "root": php_root, "script": "index.php" }
            },
            "user": "phpapp",
            "group": "phpapp",
            "limits": { "requests": 3, "timeout": 2 },
            "processes": { "max": 4, "spare": 2 },
            "queue": { "timeout": 2 }
        }
    });
    // A second listen address cannot come through `merge_json`: that would
    // replace the array and lose the port the harness connects on.
    if let Some(extra) = overrides.get("listen_extra").and_then(|v| v.as_u64()) {
        config["listen"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!(format!("127.0.0.1:{extra}")));
    }
    merge_json(&mut config, overrides);
    config.as_object_mut().unwrap().remove("listen_extra");

    let config_path = std::env::temp_dir().join(format!("test-config-{name}.json"));
    std::fs::File::create(&config_path)
        .unwrap()
        .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_proteus"))
        .arg(&config_path)
        .env("PROTEUS_PHP_MOD_PATH", php_mod_path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server binary");

    let server = TestServer {
        child,
        port,
        status_port,
    };

    let client = reqwest::Client::new();
    // Generous, because every server here forks a prototype and its spare
    // workers, and the whole suite starts many of them on the same cores at
    // once. This is contention, not the pool being slow.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("server on port {port} (status {status_port}) never became ready");
        }
        match client
            .get(format!("http://127.0.0.1:{status_port}/"))
            .timeout(Duration::from_millis(500))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => break,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    server
}

/// Unique per call, rather than hashed from a test name, where a collision
/// would silently break one of the two.
fn next_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(21000);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn merge_json(base: &mut serde_json::Value, patch: serde_json::Value) {
    if let (Some(base_map), Some(patch_map)) = (base.as_object_mut(), patch.as_object()) {
        for (k, v) in patch_map {
            if let Some(existing) = base_map.get_mut(k)
                && existing.is_object()
                && v.is_object()
            {
                merge_json(existing, v.clone());
                continue;
            }
            base_map.insert(k.clone(), v.clone());
        }
    }
}

#[tokio::test]
async fn static_file_is_served_directly() {
    let www = fixtures_dir().join("www");
    let server = start_server("static", www.to_str().unwrap(), serde_json::json!({})).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/hello.txt", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
    let body = resp.text().await.unwrap();
    assert!(body.contains("static file"));
}

/// A HEAD response's headers must match GET's with an empty body (RFC 9110
/// §9.3.2). Hyper enforces the empty body itself, so what this exercises is
/// the short-circuit that skips building the body at all - without which a
/// HEAD still pays to read and compress a file nobody receives.
#[tokio::test]
async fn head_request_matches_get_headers_with_an_empty_body() {
    let www = fixtures_dir().join("www");
    let server = start_server("head-static", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap(); // inspect real headers ourselves

    for (path, accept_encoding) in [("/hello.txt", "identity"), ("/big.txt", "gzip")] {
        let url = format!("http://127.0.0.1:{}{path}", server.port);
        let get_resp = client
            .get(&url)
            .header("Accept-Encoding", accept_encoding)
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.status(), 200);
        let get_headers = get_resp.headers().clone();
        let get_body_len = get_resp.bytes().await.unwrap().len();
        assert!(
            get_body_len > 0,
            "sanity: GET {path} must actually have a body"
        );

        let head_resp = client
            .head(&url)
            .header("Accept-Encoding", accept_encoding)
            .send()
            .await
            .unwrap();
        assert_eq!(head_resp.status(), 200);
        for header in ["content-type", "content-encoding", "etag", "content-length"] {
            assert_eq!(
                head_resp.headers().get(header),
                get_headers.get(header),
                "HEAD/GET {header} mismatch for {path}"
            );
        }
        let head_body = head_resp.bytes().await.unwrap();
        assert!(
            head_body.is_empty(),
            "HEAD {path} must have an empty body, got {} bytes",
            head_body.len()
        );
    }
}

/// The property itself - that a large file never becomes a same-sized
/// allocation - is not observable from a client, but a dropped or duplicated
/// chunk is, and a small fixture would pass such a bug by accident.
#[tokio::test]
async fn large_static_file_streams_correctly() {
    let root = std::env::temp_dir().join(format!("streaming-test-{}", next_port()));
    tokio::fs::create_dir_all(root.join("public"))
        .await
        .unwrap();
    // Not zeros: corruption goes unnoticed against an all-zero file.
    let content: Vec<u8> = (0..6 * 1024 * 1024)
        .map(|i: usize| (i % 251) as u8)
        .collect();
    tokio::fs::write(root.join("public/big.bin"), &content)
        .await
        .unwrap();

    let server = start_server("streaming", root.to_str().unwrap(), serde_json::json!({})).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/big.bin", server.port))
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        content.len().to_string(),
        "streaming still knows the exact size upfront - a stat(), not a full read"
    );
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "Accept-Encoding: identity must not compress"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), content.len());
    assert_eq!(
        body.as_ref(),
        content.as_slice(),
        "streamed content must match the source file exactly"
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
}

/// A large static file is compressed by the streaming encoder rather than
/// buffered first. The compressed size is unknowable ahead of time, so this
/// checks chunked framing rather than a `Content-Length`.
#[tokio::test]
async fn large_static_file_streams_compressed_when_accepted() {
    let root = std::env::temp_dir().join(format!("streaming-compressed-test-{}", next_port()));
    tokio::fs::create_dir_all(root.join("public"))
        .await
        .unwrap();
    // Must be genuinely compressible, or the test proves nothing.
    let content = "the quick brown fox jumps over the lazy dog\n".repeat(200_000);
    tokio::fs::write(root.join("public/big.txt"), &content)
        .await
        .unwrap();

    let server = start_server(
        "streaming-compressed",
        root.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap(); // inspect the raw header/body ourselves
    let resp = client
        .get(format!("http://127.0.0.1:{}/big.txt", server.port))
        .header("Accept-Encoding", "zstd")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "zstd");
    assert!(
        resp.headers().get("content-length").is_none(),
        "compressed streaming size isn't known upfront"
    );

    let compressed = resp.bytes().await.unwrap();
    let decoded = String::from_utf8(zstd::decode_all(compressed.as_ref()).unwrap()).unwrap();
    assert_eq!(decoded, content);

    let _ = tokio::fs::remove_dir_all(&root).await;
}

/// Range and conditional GET end to end, including that an out-of-bounds
/// range is a 416 rather than silently served as if absent.
#[tokio::test]
async fn static_file_range_and_conditional_requests() {
    let root = std::env::temp_dir().join(format!("range-test-{}", next_port()));
    tokio::fs::create_dir_all(root.join("public"))
        .await
        .unwrap();
    let content: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(root.join("public/data.txt"), &content)
        .await
        .unwrap();

    let server = start_server("range", root.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/data.txt", server.port);

    // Uncompressed, so the ETag must be strong.
    let plain = client
        .get(&url)
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .unwrap();
    assert_eq!(plain.status(), 200);
    assert_eq!(plain.headers().get("accept-ranges").unwrap(), "bytes");
    assert_eq!(plain.headers().get("vary").unwrap(), "Accept-Encoding");
    let etag = plain
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        !etag.starts_with("W/"),
        "identity response must carry a strong ETag: {etag}"
    );
    let last_modified = plain
        .headers()
        .get("last-modified")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // No Vary: a range is always identity bytes, so claiming it varies only
    // makes a cache partition ranges for nothing.
    let ranged = client
        .get(&url)
        .header("Range", "bytes=100-199")
        .send()
        .await
        .unwrap();
    assert_eq!(ranged.status(), 206);
    assert_eq!(
        *ranged.headers().get("content-range").unwrap(),
        format!("bytes 100-199/{}", content.len())
    );
    assert_eq!(ranged.headers().get("content-length").unwrap(), "100");
    assert!(ranged.headers().get("vary").is_none());
    let body = ranged.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[100..200]);

    let suffix = client
        .get(&url)
        .header("Range", "bytes=-50")
        .send()
        .await
        .unwrap();
    assert_eq!(suffix.status(), 206);
    let start = content.len() - 50;
    assert_eq!(
        *suffix.headers().get("content-range").unwrap(),
        format!("bytes {start}-{}/{}", content.len() - 1, content.len())
    );
    assert_eq!(suffix.bytes().await.unwrap().as_ref(), &content[start..]);

    // Still Vary'd: a 304 stands in for whatever 200 would have been.
    let not_modified = client
        .get(&url)
        .header("If-None-Match", &etag)
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .unwrap();
    assert_eq!(not_modified.status(), 304);
    assert_eq!(
        not_modified.headers().get("vary").unwrap(),
        "Accept-Encoding"
    );
    assert!(not_modified.bytes().await.unwrap().is_empty());

    // Compares no ETags, so the encoding it would negotiate is irrelevant.
    let not_modified2 = client
        .get(&url)
        .header("If-Modified-Since", &last_modified)
        .send()
        .await
        .unwrap();
    assert_eq!(not_modified2.status(), 304);
    assert_eq!(
        not_modified2.headers().get("vary").unwrap(),
        "Accept-Encoding"
    );

    // Out-of-bounds Range: 416, not a silent full/200 response. No Vary,
    // same reasoning as the 206 case above.
    let unsatisfiable = client
        .get(&url)
        .header("Range", "bytes=999999-9999999")
        .send()
        .await
        .unwrap();
    assert_eq!(unsatisfiable.status(), 416);
    assert_eq!(
        *unsatisfiable.headers().get("content-range").unwrap(),
        format!("bytes */{}", content.len())
    );
    assert!(unsatisfiable.headers().get("vary").is_none());

    // If-Range with a still-current etag: Range is honored, same as if
    // If-Range weren't sent at all.
    let if_range_fresh = client
        .get(&url)
        .header("Range", "bytes=100-199")
        .header("If-Range", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(if_range_fresh.status(), 206);
    assert_eq!(
        if_range_fresh.bytes().await.unwrap().as_ref(),
        &content[100..200]
    );

    // If-Range with a stale etag: the precondition fails, so Range is
    // ignored entirely and the full representation comes back as 200 -
    // not a 206 of the wrong bytes, and not a 412/416 either.
    let if_range_stale = client
        .get(&url)
        .header("Range", "bytes=100-199")
        .header("If-Range", "W/\"stale-0\"")
        .send()
        .await
        .unwrap();
    assert_eq!(if_range_stale.status(), 200);
    assert_eq!(
        if_range_stale.bytes().await.unwrap().as_ref(),
        content.as_slice()
    );

    // No Accept-Ranges: a range is meaningless against an on-the-fly
    // compressed representation. The ETag keeps its opaque tag but is
    // weakened rather than made encoding-specific.
    let no_gzip_client = reqwest::Client::builder().no_gzip().build().unwrap();
    let compressed = no_gzip_client
        .get(&url)
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(
        compressed.headers().get("content-encoding").unwrap(),
        "gzip"
    );
    assert!(compressed.headers().get("accept-ranges").is_none());
    assert_eq!(compressed.headers().get("vary").unwrap(), "Accept-Encoding");
    let gzip_etag = compressed
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        gzip_etag,
        format!("W/{etag}"),
        "same opaque tag as the identity response's, just weakened"
    );

    // That weakened ETag can never satisfy If-Range (strong comparison
    // only) - even reused immediately, on the same file, with no actual
    // staleness at all.
    let if_range_weak = client
        .get(&url)
        .header("Range", "bytes=100-199")
        .header("If-Range", &gzip_etag)
        .send()
        .await
        .unwrap();
    assert_eq!(
        if_range_weak.status(),
        200,
        "a weak If-Range must never be honored, per RFC 9110 §13.1.5"
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
}

/// A file too small to ever be compressed cannot vary by encoding, so
/// claiming it does only makes a cache partition it for nothing. Its ETag
/// must also stay strong.
#[tokio::test]
async fn small_static_file_gets_no_vary_header() {
    let root = std::env::temp_dir().join(format!("vary-test-{}", next_port()));
    tokio::fs::create_dir_all(root.join("public"))
        .await
        .unwrap();
    tokio::fs::write(root.join("public/tiny.txt"), b"too small to compress")
        .await
        .unwrap();

    let server = start_server("vary", root.to_str().unwrap(), serde_json::json!({})).await;
    let url = format!("http://127.0.0.1:{}/tiny.txt", server.port);

    let resp = reqwest::Client::new()
        .get(&url)
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "well under min_size_bytes, must not compress"
    );
    assert!(
        resp.headers().get("vary").is_none(),
        "never compression-eligible, so it can't vary by encoding"
    );
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap();
    assert!(
        !etag.starts_with("W/"),
        "an ineligible response's ETag must stay strong: {etag}"
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
}

/// PHP picks 303 over 302 for a bare `header("Location: ...")` only when the
/// protocol is above HTTP/1.0 and the method is not GET or HEAD. This server
/// always declares HTTP/1.1, so that branch is reachable whatever the client
/// actually spoke.
#[tokio::test]
async fn php_location_redirect_uses_303_for_post_and_302_for_get() {
    let www = fixtures_dir().join("www");
    let server = start_server("redirect", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let get_resp = client
        .get(format!("http://127.0.0.1:{}/redirect", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_resp.status(),
        302,
        "GET with no explicit code stays 302"
    );

    let post_resp = client
        .post(format!("http://127.0.0.1:{}/redirect", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(
        post_resp.status(),
        303,
        "POST with no explicit code becomes 303 - needs proto_num > 1000"
    );

    assert_eq!(get_resp.headers().get("location").unwrap(), "/done");
    assert_eq!(post_resp.headers().get("location").unwrap(), "/done");
}

/// Many separate echo() calls must reassemble byte-perfect, compressed or
/// not. That neither process held the whole response at once is not
/// observable from a client, but a reassembly bug is.
#[tokio::test]
async fn php_streams_a_large_multi_chunk_response_correctly() {
    let www = fixtures_dir().join("www");
    let server = start_server("php-stream", www.to_str().unwrap(), serde_json::json!({})).await;

    let expected: String = "0123456789abcdef".repeat(256).repeat(1000);

    // Uncompressed - real Content-Length isn't sent (`build_php_stream_
    // response` doesn't know the total ahead of time), but the
    // reassembled body must still match exactly.
    let uncompressed = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/stream-big", server.port))
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .unwrap();
    assert_eq!(uncompressed.status(), 200);
    assert!(uncompressed.headers().get("content-encoding").is_none());
    let body = uncompressed.text().await.unwrap();
    assert_eq!(body, expected);

    // Compressed - same negotiation as a buffered PHP response
    // (`pick_encoding_for_stream`), just applied to a live stream.
    let client = reqwest::Client::builder().no_gzip().build().unwrap();
    let compressed = client
        .get(format!("http://127.0.0.1:{}/stream-big", server.port))
        .header("Accept-Encoding", "zstd")
        .send()
        .await
        .unwrap();
    assert_eq!(compressed.status(), 200);
    assert_eq!(
        compressed.headers().get("content-encoding").unwrap(),
        "zstd"
    );
    let compressed_bytes = compressed.bytes().await.unwrap();
    let decoded = String::from_utf8(zstd::decode_all(compressed_bytes.as_ref()).unwrap()).unwrap();
    assert_eq!(decoded, expected);
}

/// The opposite failure mode: one echo() of a multi-MB string must not
/// become one arbitrarily large frame, which is no better than not streaming
/// and large enough to be rejected outright as too big for the ring.
#[tokio::test]
async fn php_streams_a_single_huge_echo_call_correctly() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "php-stream-one-echo",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let expected: String = "0123456789abcdef".repeat(256 * 1000);

    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/stream-one-echo", server.port))
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, expected);
}

/// A header set too big for one ring frame must not fail the request.
/// Covers both shapes the splitter handles - many entries, and one large
/// one - end to end through a real worker.
///
/// Few, large cookies rather than many small ones: a real HTTP client caps
/// how many distinct headers it will parse at all, so the total size has to
/// come from their size. The unit tests cover the many-tiny-entries shape,
/// unconstrained by any client limit.
#[tokio::test]
async fn php_response_with_oversized_headers_reaches_client_intact() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "php-many-headers",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/many-headers?n=50&vsize=6000&csp=1",
            server.port
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
    assert_eq!(
        cookies.len(),
        50,
        "expected all 50 Set-Cookie headers to survive the split/reassembly"
    );
    assert_eq!(
        cookies[0].to_str().unwrap(),
        format!("cookie_0={}; Path=/", "v".repeat(6000))
    );
    assert_eq!(
        cookies[49].to_str().unwrap(),
        format!("cookie_49={}; Path=/", "v".repeat(6000))
    );

    let csp = resp
        .headers()
        .get("content-security-policy")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(csp, expected_csp());

    let body = resp.text().await.unwrap();
    assert_eq!(body, "headers-ok n=50\n");
}

/// Matches the fixture's `implode('; ', ...)` construction exactly - see
/// `/many-headers`'s own doc comment in index.php for why it's `implode`,
/// not a trailing-separator `str_repeat`.
fn expected_csp() -> String {
    vec!["default-src 'self'"; 5000].join("; ")
}

/// One large entry among small ones must exercise the split on its own, and
/// the same pooled worker must survive a normal request right after one that
/// needed splitting.
#[tokio::test]
async fn php_response_with_one_oversized_header_value_reaches_client_intact() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "php-one-big-header",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/many-headers?n=5&csp=1",
            server.port
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get_all("set-cookie").iter().count(), 5);
    assert_eq!(
        resp.headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap(),
        expected_csp()
    );
    assert_eq!(resp.text().await.unwrap(), "headers-ok n=5\n");

    // The same worker's very next response, ordinary-sized headers - proves
    // the split path didn't leave the ring/worker in a bad state.
    let ordinary = client
        .get(format!("http://127.0.0.1:{}/", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(ordinary.status(), 200);
    assert!(ordinary.text().await.unwrap().starts_with("PHP response"));
}

/// `header()` only rejects CR/LF, not every control byte - a reflected one
/// must not tear the connection down: the bad header is dropped, and
/// headers around it still arrive.
#[tokio::test]
async fn a_script_reflected_control_byte_in_a_header_does_not_break_the_response() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "reflect-header",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/reflect-header?v=%01",
            server.port
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "an invalid reflected header must not fail the whole response"
    );
    assert_eq!(resp.headers().get("x-before").unwrap(), "still-here");
    assert!(
        resp.headers().get("x-reflected").is_none(),
        "the invalid header itself must be dropped"
    );
    assert_eq!(
        resp.headers().get("x-after").unwrap(),
        "also-here",
        "a header queued after the bad one must still reach the client"
    );
    assert_eq!(resp.text().await.unwrap(), "reflected\n");

    // The same worker's very next response - proves this didn't leave the
    // ring/worker or the connection in a bad state.
    let ordinary = client
        .get(format!("http://127.0.0.1:{}/", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(ordinary.status(), 200);
    assert!(ordinary.text().await.unwrap().starts_with("PHP response"));
}

/// `rate_limit.user_agent` is a filter: only requests whose User-Agent
/// matches get counted or limited at all, everything else is untouched.
#[tokio::test]
async fn rate_limit_only_applies_to_matching_user_agents_and_recovers_with_retry_after() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "rate-limit",
        www.to_str().unwrap(),
        serde_json::json!({
            "rate_limit": { "requests": 2, "period_seconds": 60, "user_agent": ["*GPTBot*"] }
        }),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/", server.port);

    // Burst capacity of 2 for a matching User-Agent.
    for n in 1..=2 {
        let resp = client
            .get(&url)
            .header("User-Agent", "Mozilla/5.0 (compatible; GPTBot/1.0)")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "request {n} should still be within budget"
        );
    }

    // The third exceeds the burst.
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (compatible; GPTBot/1.0)")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    assert_eq!(resp.headers().get("retry-after").unwrap(), "60");
    assert!(resp.text().await.unwrap().contains("too many requests"));

    // A non-matching User-Agent is never subject to this limiter at all -
    // exhausting the GPTBot budget above must not have touched it.
    for _ in 0..5 {
        let resp = client
            .get(&url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "a non-matching User-Agent must never be rate-limited"
        );
    }
}

/// End to end over a real socket: the unit tests pin the resolver, this pins
/// that `handle()` actually feeds the limiter the resolved identity. A client
/// that is not a configured trusted proxy must share one bucket no matter
/// what it forwards, and the same server must still split clients that a
/// trusted proxy vouches for.
#[tokio::test]
async fn x_forwarded_for_cannot_buy_extra_rate_limit_budget() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "rate-limit-spoof",
        www.to_str().unwrap(),
        serde_json::json!({
            "rate_limit": { "requests": 3, "period_seconds": 60, "user_agent": ["*GPTBot*"] }
        }),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/", server.port);

    let mut statuses = Vec::new();
    for n in 1..=6 {
        let resp = client
            .get(&url)
            .header("User-Agent", "GPTBot/1.0")
            .header("X-Forwarded-For", format!("1.1.1.{n}"))
            .send()
            .await
            .unwrap();
        statuses.push(resp.status().as_u16());
    }
    assert_eq!(
        statuses,
        vec![200, 200, 200, 429, 429, 429],
        "trusted_proxies is empty, so a rotating X-Forwarded-For must buy nothing"
    );
}

/// The other half: with the peer configured as a trusted proxy, its
/// `X-Forwarded-For` decides the bucket, so one client exhausting its budget
/// must not spend another's.
#[tokio::test]
async fn a_trusted_proxy_gets_a_bucket_per_forwarded_client() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "rate-limit-proxied",
        www.to_str().unwrap(),
        serde_json::json!({
            "trusted_proxies": ["127.0.0.1/32"],
            "rate_limit": { "requests": 2, "period_seconds": 60, "user_agent": ["*GPTBot*"] }
        }),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/", server.port);

    let get = async |forwarded_for: &str| {
        client
            .get(&url)
            .header("User-Agent", "GPTBot/1.0")
            .header("X-Forwarded-For", forwarded_for)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    };

    assert_eq!(get("198.51.100.20").await, 200);
    assert_eq!(get("198.51.100.20").await, 200);
    assert_eq!(
        get("198.51.100.20").await,
        429,
        "the first forwarded client's own burst must run out"
    );
    assert_eq!(
        get("198.51.100.21").await,
        200,
        "a different forwarded client must have its own budget"
    );
}

/// A proxy serves many clients down one keep-alive connection, so caching a
/// resolved identity per connection would report them all as the first.
#[tokio::test]
async fn one_keep_alive_connection_resolves_each_request_forwarded_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "xff-per-request",
        www.to_str().unwrap(),
        serde_json::json!({ "trusted_proxies": ["127.0.0.1/32"] }),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();

    let mut seen = Vec::new();
    for client in ["198.51.100.20", "198.51.100.21"] {
        stream
            .write_all(
                format!(
                    "GET /app HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: {client}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // The connection stays open for the next request, so read_to_end
        // would block until the idle timeout.
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0u8; 1024];
            let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
                .await
                .expect("timed out waiting for a response")
                .unwrap();
            assert_ne!(n, 0, "server closed the connection mid-exchange");
            buf.extend_from_slice(&chunk[..n]);
            if String::from_utf8_lossy(&buf).contains("REMOTE_ADDR=") {
                break;
            }
        }
        let body = String::from_utf8_lossy(&buf).to_string();
        let addr = body
            .split("REMOTE_ADDR=")
            .nth(1)
            .and_then(|rest| rest.split('\n').next())
            .unwrap()
            .trim()
            .to_string();
        seen.push(addr);
    }

    assert_eq!(
        seen,
        vec!["198.51.100.20".to_string(), "198.51.100.21".to_string()],
        "the second request on the same connection must not inherit the first's client"
    );
}

/// `index` mode resolves the script from the URL, so an ungated target hands
/// the interpreter any regular file under `root`: an uploaded `.png` is
/// remote code execution, and a file without PHP in it is echoed verbatim.
#[tokio::test]
async fn only_configured_script_extensions_reach_the_interpreter() {
    let www = fixtures_dir().join("uploads-www");
    let server = start_server(
        "ext-gate",
        www.to_str().unwrap(),
        serde_json::json!({
            "routes": [ { "match": {}, "action": "php", "target": "default" } ],
            "php": { "targets": { "default": {
                "root": www.to_str().unwrap(), "script": null, "index": "index.php"
            } } }
        }),
    )
    .await;
    let base = format!("http://127.0.0.1:{}", server.port);

    let app = reqwest::get(format!("{base}/")).await.unwrap();
    assert_eq!(app.status(), 200);
    assert!(
        app.text().await.unwrap().contains("APP OK"),
        "the real entrypoint must still run"
    );

    for path in ["/uploads/avatar.png", "/.env", "/uploads/note.txt"] {
        let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert_eq!(status, 404, "{path} must not resolve to a script");
        assert!(
            !body.contains("PHP-EXECUTED"),
            "{path} was executed as PHP: {body}"
        );
        assert!(
            !body.contains("hunter2"),
            "{path} leaked its contents: {body}"
        );
    }
}

/// The gate is a configured list, not a hardcoded `.php`, so a legacy app can
/// opt an extension in - and opting one in must not open the rest.
#[tokio::test]
async fn script_extensions_is_configurable_without_widening_the_rest() {
    let www = fixtures_dir().join("uploads-www");
    let server = start_server(
        "ext-gate-phtml",
        www.to_str().unwrap(),
        serde_json::json!({
            "routes": [ { "match": {}, "action": "php", "target": "default" } ],
            "php": {
                "script_extensions": ["php", "phtml"],
                "targets": { "default": {
                    "root": www.to_str().unwrap(), "script": null, "index": "index.php"
                } }
            }
        }),
    )
    .await;
    let base = format!("http://127.0.0.1:{}", server.port);

    let legacy = reqwest::get(format!("{base}/legacy.phtml")).await.unwrap();
    assert_eq!(legacy.status(), 200);
    assert!(legacy.text().await.unwrap().contains("PHTML-EXECUTED"));

    let png = reqwest::get(format!("{base}/uploads/avatar.png"))
        .await
        .unwrap();
    assert_eq!(png.status(), 404, "listing phtml must not admit png too");
}

/// Read back from the live worker rather than asserted at the call site: the
/// flag has to survive the prototype's `execve` and the `fork` that makes the
/// worker, and only `/proc` can say that it did.
#[tokio::test]
async fn workers_run_with_no_new_privs_by_default() {
    let www = fixtures_dir().join("www");
    let server = start_server("nnp", www.to_str().unwrap(), serde_json::json!({})).await;

    let body = reqwest::get(format!("http://127.0.0.1:{}/", server.port))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let pid: u32 = body
        .split("worker pid=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|d| d.parse().ok())
        .unwrap_or_else(|| panic!("no worker pid in response: {body}"));

    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let line = status
        .lines()
        .find(|l| l.starts_with("NoNewPrivs:"))
        .unwrap_or_else(|| panic!("kernel reports no NoNewPrivs field for pid {pid}"));
    assert_eq!(
        line.split_whitespace().nth(1),
        Some("1"),
        "worker {pid} did not inherit PR_SET_NO_NEW_PRIVS: {line}"
    );
}

/// The escape hatch has to actually reach the worker, or an operator whose
/// `mail()` needs a setgid helper has no way out.
#[tokio::test]
async fn no_new_privs_can_be_turned_off() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "nnp-off",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "no_new_privs": false } }),
    )
    .await;

    let body = reqwest::get(format!("http://127.0.0.1:{}/", server.port))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let pid: u32 = body
        .split("worker pid=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|d| d.parse().ok())
        .unwrap();

    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let line = status
        .lines()
        .find(|l| l.starts_with("NoNewPrivs:"))
        .unwrap();
    assert_eq!(line.split_whitespace().nth(1), Some("0"), "got: {line}");
}

/// A script that flushes early and then works on must not have its first
/// chunk held back until it finishes - which is what collecting a whole
/// response before answering would do.
#[tokio::test]
async fn an_early_flush_still_reaches_the_client_before_the_script_ends() {
    let www = fixtures_dir().join("www");
    let server = start_server("early-flush", www.to_str().unwrap(), serde_json::json!({})).await;

    let started = std::time::Instant::now();
    // A whole-response buffer would push the first chunk out only at the end.
    let mut resp = reqwest::get(format!("http://127.0.0.1:{}/flush-stream", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let first = resp.chunk().await.unwrap().expect("no body chunk arrived");
    let first_at = started.elapsed();

    let mut body = String::from_utf8_lossy(&first).to_string();
    while let Some(chunk) = resp.chunk().await.unwrap() {
        body.push_str(&String::from_utf8_lossy(&chunk));
    }
    let total = started.elapsed();

    assert!(
        body.contains("chunk-0") && body.contains("chunk-4"),
        "the whole stream must still arrive: {body:?}"
    );
    assert!(
        total >= Duration::from_millis(700),
        "the script sleeps between chunks, so this should not have been instant: {total:?}"
    );
    assert!(
        first_at < total / 2,
        "the first chunk arrived at {first_at:?} of a {total:?} response - it was buffered, \
         not streamed"
    );
}

/// Hitting the pending-headers cap must not simply abandon the response ring
/// while the worker, still mid-write, blocks forever in an untimed wait with
/// nothing left to free space or kill it. It is dropped from `/status`
/// immediately, so such a leak would leave no operator-visible trace.
///
/// Sized so the run total passes the cap while every individual entry stays
/// well under the per-frame budget, exercising the cap rather than the
/// unrelated single-entry-too-big path.
#[tokio::test]
async fn oversized_headers_run_past_the_cap_does_not_leak_a_blocked_worker() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "php-headers-cap",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let pids_before = worker_pids(&server).await;
    assert!(
        !pids_before.is_empty(),
        "expected pre-spawned spare workers before the request"
    );
    // By process instance, not pid: the pool keeps spawning replacements
    // while this polls, and a healthy one can legitimately reuse the pid.
    let start_times_before: std::collections::HashMap<i64, String> = pids_before
        .iter()
        .map(|&p| {
            (
                p,
                process_start_time(p).expect("pre-existing worker must be readable"),
            )
        })
        .collect();

    // Both the first attempt and `dispatch()`'s retry hit the exact same
    // deterministic PHP script, so both involved workers must end up
    // truly dead - not just the first one.
    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/many-headers?n=300&vsize=60000",
            server.port
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        500,
        "both attempts should exhaust and the request should fail cleanly, not hang"
    );

    // A pid leaves `/status` before the kill is necessarily even delivered,
    // so this only proves the bookkeeping updated.
    let detect_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let consumed = loop {
        let current = worker_pids(&server).await;
        let consumed: Vec<i64> = pids_before
            .iter()
            .copied()
            .filter(|p| !current.contains(p))
            .collect();
        if !consumed.is_empty() || tokio::time::Instant::now() > detect_deadline {
            break consumed;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        !consumed.is_empty(),
        "expected at least one pre-existing worker to have been consumed by the request"
    );

    // Poll to actual death rather than wait a fixed delay: the reap cadence
    // is a steady-state figure, and the whole suite running at once pushes it
    // well past that. A leaked process never dies however long
    // this polls, so a generous deadline costs time only when genuinely
    // broken.
    let reap_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for pid in consumed {
        let expected_start = start_times_before[&pid].clone();
        loop {
            // Fingerprint, not bare existence: a healthy replacement can
            // reuse this pid, so only a start-time mismatch proves the
            // original instance is gone.
            if process_start_time(pid).as_deref() != Some(expected_start.as_str()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < reap_deadline,
                "worker pid={pid} is still alive (same process instance, start_time={expected_start}) - it leaked \
                 as a permanently blocked process instead of being killed. /proc/{pid}/status:\n{}\n\
                 /proc/{pid}/wchan: {}",
                std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default(),
                std::fs::read_to_string(format!("/proc/{pid}/wchan")).unwrap_or_default(),
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// A stable fingerprint for one process instance, unlike a pid, which the OS
/// may reuse the moment this one exits.
///
/// The `comm` field is parenthesized and may itself contain spaces or
/// parens, so the field has to be located from the last `)`.
fn process_start_time(pid: i64) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    after_comm.split_whitespace().nth(19).map(str::to_string) // field 22 = fields[3..].nth(19)
}

/// The real uid (`/proc/[pid]/status`'s `Uid:` line, first of its four
/// space-separated values - real/effective/saved/filesystem) `pid` is
/// actually running as right now.
fn process_real_uid(pid: i64) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("Uid:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

async fn status_json(server: &TestServer) -> serde_json::Value {
    reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn worker_pids(server: &TestServer) -> Vec<i64> {
    status_json(server).await["php"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["pid"].as_i64().unwrap())
        .collect()
}

/// With no user or group configured, the worker must run as this test
/// process's own identity - proving no setuid happened at all, rather than
/// happening to target the same uid. `null` is how the merge asks for absent.
#[tokio::test]
async fn php_user_group_omitted_inherits_masters_own_identity() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "no-privilege-drop",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "user": null, "group": null, "processes": { "max": 1, "spare": 1 } } }),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let pids = worker_pids(&server).await;
    assert_eq!(pids.len(), 1, "expected exactly one worker: {pids:?}");
    let worker_uid = process_real_uid(pids[0])
        .expect("worker process must still be alive and readable via /proc");
    let own_uid = nix::unistd::getuid().as_raw();
    assert_eq!(
        worker_uid, own_uid,
        "worker must inherit this test process's own uid when php.user/group are omitted"
    );
}

#[tokio::test]
async fn php_dispatch_falls_back_from_missing_static_file() {
    let www = fixtures_dir().join("www");
    let server = start_server("php-fallback", www.to_str().unwrap(), serde_json::json!({})).await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/does-not-exist-as-a-file",
        server.port
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.starts_with("PHP response"), "got: {body}");
}

/// A three-deep chain, so the walk must advance past more than one hop -
/// which a single fallback cannot distinguish from a bug that checks only
/// the first.
#[tokio::test]
async fn php_dispatch_falls_back_through_a_multi_level_static_chain() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "multi-fallback",
        www.to_str().unwrap(),
        serde_json::json!({
            "routes": [
                { "match": {}, "action": "static", "root": "/nonexistent-root-1",
                  "fallback": { "action": "static", "root": "/nonexistent-root-2",
                    "fallback": { "action": "php", "target": "default" } } }
            ]
        }),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.starts_with("PHP response"),
        "expected the chain to fall through both missing static roots to php: {body}"
    );
}

/// Several named entrypoints sharing one pool, covering both target modes.
/// Enough concurrent requests spread across all three that a per-target pool
/// would exceed the worker cap the final assertion checks.
#[tokio::test]
async fn targets_share_one_pool_with_independent_entrypoints() {
    let www = fixtures_dir().join("www");
    let api_root = fixtures_dir().join("targets/api/public");
    // Resolution is not mount-relative: this target's root is one level
    // above its files, so the full request path resolves under it unchanged.
    let legacy_root = fixtures_dir().join("targets");

    let server = start_server(
        "targets",
        www.to_str().unwrap(),
        serde_json::json!({
            "routes": [
                { "match": { "uri": ["/api/*"] }, "action": "php", "target": "api" },
                { "match": { "uri": ["/legacy/*"] }, "action": "php", "target": "legacy" },
                { "match": {}, "action": "static",
                  "root": format!("{}/public", www.to_str().unwrap()),
                  "fallback": { "action": "php", "target": "default" } }
            ],
            "php": {
                "targets": {
                    "api": { "root": api_root.to_str().unwrap(), "script": "index.php" },
                    "legacy": { "root": legacy_root.to_str().unwrap() }
                },
                "processes": { "max": 2, "spare": 1 }
            }
        }),
    )
    .await;

    let client = reqwest::Client::new();

    // "script" target: single front-controller, PATH_INFO = whole URL path
    // - same convention the default (no-target) entrypoint already used.
    let resp = client
        .get(format!("http://127.0.0.1:{}/api/whoami", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("api target pid="), "got: {body}");
    assert!(body.contains("SCRIPT_NAME=/index.php"), "got: {body}");
    assert!(body.contains("PATH_INFO=/api/whoami"), "got: {body}");
    assert!(
        body.contains(&format!("DOCUMENT_ROOT={}", api_root.to_str().unwrap())),
        "got: {body}"
    );

    // "index" target: direct URL -> file match, no PATH_INFO.
    let resp = client
        .get(format!("http://127.0.0.1:{}/legacy/foo.php", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("legacy foo pid="), "got: {body}");
    assert!(body.contains("SCRIPT_NAME=/legacy/foo.php"), "got: {body}");
    // PATH_INFO is always set (possibly empty), never absent - same
    // convention `php_receives_server_vars_and_environment` already relies
    // on for the default entrypoint.
    assert!(body.contains("PATH_INFO=\n"), "got: {body}");

    // "index" target: PATH_INFO split after the matched script.
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/legacy/sub/handler.php/extra/path",
            server.port
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("SCRIPT_NAME=/legacy/sub/handler.php"),
        "got: {body}"
    );
    assert!(body.contains("PATH_INFO=/extra/path"), "got: {body}");

    // "index" target: directory-style request appends the target's index.
    let resp = client
        .get(format!("http://127.0.0.1:{}/legacy/dir/", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("legacy dir index pid="), "got: {body}");

    // "index" target: nothing on disk matches -> a plain 404, not a PHP
    // error (resolve_index_target returning None short-circuits before any
    // worker is ever dispatched to).
    let resp = client
        .get(format!("http://127.0.0.1:{}/legacy/nope.php", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Default (no-target) entrypoint keeps working unchanged, same server.
    let resp = client
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut handles = Vec::new();
    for i in 0..9 {
        let client = client.clone();
        let port = server.port;
        let path = match i % 3 {
            0 => "/api/whoami",
            1 => "/legacy/foo.php",
            _ => "/app",
        };
        handles.push(tokio::spawn(async move {
            client
                .get(format!("http://127.0.0.1:{port}{path}"))
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), reqwest::StatusCode::OK);
    }

    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert!(
        status["php"]["processes"]["total"].as_u64().unwrap() <= 2,
        "targets must share one pool, not spawn one per target: {status}"
    );
}

/// Two virtual targets on one listener, selected by `Host`, including the
/// case-insensitivity the matcher itself does not provide.
#[tokio::test]
async fn host_routing_selects_between_two_virtual_targets() {
    let www = fixtures_dir().join("www");
    let api_root = fixtures_dir().join("targets/api/public");
    let server = start_server(
        "host-routing",
        www.to_str().unwrap(),
        serde_json::json!({
            "routes": [
                { "match": { "host": ["api.example.test"] }, "action": "php", "target": "api" },
                { "match": {}, "action": "php", "target": "default" }
            ],
            "php": {
                "targets": { "api": { "root": api_root.to_str().unwrap(), "script": "index.php" } }
            }
        }),
    )
    .await;

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("http://127.0.0.1:{}/", server.port))
        .header("Host", "api.example.test")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("api target pid="),
        "Host: api.example.test should route to the api target: {body}"
    );

    let resp = client
        .get(format!("http://127.0.0.1:{}/", server.port))
        .header("Host", "other.example.test")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("PHP response"),
        "any other Host should fall through to the catch-all default target: {body}"
    );

    // Host headers aren't case-sensitive - the resolved value gets
    // lowercased before matching (config patterns are documented as
    // lowercase-only), so a mixed-case client Host must still hit "api".
    let resp = client
        .get(format!("http://127.0.0.1:{}/", server.port))
        .header("Host", "API.Example.Test")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("api target pid="),
        "Host matching must be case-insensitive: {body}"
    );
}

/// LIFO, not FIFO: two workers finish at deliberately different times, and
/// the next request must land on whichever went idle last.
#[tokio::test]
async fn idle_workers_are_reused_in_lifo_order() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "lifo-reuse",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 2, "spare": 0 } } }),
    )
    .await;
    let client = reqwest::Client::new();

    fn extract_pid(body: &str) -> String {
        body.split("pid=")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .unwrap()
            .to_string()
    }

    // Both dispatched before either finishes, so two distinct workers are
    // used. The gap between them is wide enough that which goes idle first is
    // never in doubt under scheduler jitter.
    let slow = tokio::spawn({
        let client = client.clone();
        let url = format!("http://127.0.0.1:{}/app?delay_ms=500", server.port);
        async move { client.get(url).send().await.unwrap().text().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let fast_body = client
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let fast_pid = extract_pid(&fast_body);

    let slow_body = slow.await.unwrap();
    let slow_pid = extract_pid(&slow_body);
    assert_ne!(
        slow_pid, fast_pid,
        "the two concurrent requests must have landed on two distinct workers"
    );

    // `fast`'s worker went idle first, `slow`'s worker went idle second
    // (most recent) - LIFO must pick `slow`'s worker next, not `fast`'s.
    let next_body = client
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        extract_pid(&next_body),
        slow_pid,
        "LIFO reuse must pick the most-recently-idled worker"
    );
}

/// A script-set `Content-Length` disagreeing with the real output makes the
/// framing self-inconsistent, and on a reused connection the leftover or
/// missing bytes corrupt whatever the client reads next. It must be stripped,
/// and the next request on that connection must come back intact.
#[tokio::test]
async fn php_script_content_length_mismatch_does_not_desync_the_connection() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "bad-content-length",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::new(); // one client -> connection reuse across requests

    for (path, expected_body) in [
        (
            "/bad-content-length-under",
            "much more than two bytes of actual body\n",
        ),
        ("/bad-content-length-over", "hi"),
    ] {
        let resp = client
            .get(format!("http://127.0.0.1:{}{path}", server.port))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert!(
            resp.headers().get("content-length").is_none(),
            "the script's bogus Content-Length must never reach the wire: {path}"
        );
        let body = resp.text().await.unwrap();
        assert_eq!(
            body, expected_body,
            "the full real body must be delivered, never truncated to the script's claimed length: {path}"
        );

        // The very next request on this same (reused) connection must be
        // completely uncorrupted - proves no leftover bytes from the
        // mismatched response leaked onto it.
        let follow_up = client
            .get(format!("http://127.0.0.1:{}/app", server.port))
            .send()
            .await
            .unwrap();
        assert_eq!(follow_up.status(), 200);
        let follow_up_body = follow_up.text().await.unwrap();
        assert!(
            follow_up_body.starts_with("PHP response, worker pid="),
            "connection desync after {path}: got {follow_up_body:?}"
        );
    }
}

/// php_request_startup/shutdown-per-call php-mod fix works under real HTTP
/// traffic), then recycles after limits.requests=3 and a *different* pid
/// takes over - without ever surfacing an error to the client.
#[tokio::test]
async fn worker_is_reused_then_recycles_without_client_visible_errors() {
    let www = fixtures_dir().join("www");
    let server = start_server("recycle", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::new();

    let mut pids = Vec::new();
    for _ in 0..8 {
        let resp = client
            .get(format!("http://127.0.0.1:{}/app", server.port))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "recycling must never surface as a client error"
        );
        let body = resp.text().await.unwrap();
        let pid = body
            .split("pid=")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .unwrap()
            .to_string();
        pids.push(pid);
    }
    let distinct: std::collections::HashSet<_> = pids.iter().collect();
    assert!(
        distinct.len() >= 2,
        "expected at least one recycle (limits.requests=3, 8 requests): pids={pids:?}"
    );
}

/// A worker that never responds gets SIGKILLed and the client gets 504 -
/// not a hang, not a connection reset.
#[tokio::test]
async fn watchdog_kills_hung_worker_and_returns_504() {
    let stuck = fixtures_dir().join("stuck-www");
    let server = start_server(
        "watchdog",
        stuck.to_str().unwrap(),
        serde_json::json!({ "php": { "limits": { "timeout": 1 } } }),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/anything", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 504);

    // Pool must still be healthy afterward - status endpoint alive, and it
    // reports at least one watchdog kill.
    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert!(
        status["php"]["counters"]["watchdog_kills"]
            .as_u64()
            .unwrap()
            >= 1
    );
}

/// The worker dies after it has already streamed real bytes, so the response
/// is mid-forward rather than still awaiting its first frame. The client must
/// see the connection end rather than hang, and the pool must still serve
/// afterwards.
///
/// A failed read of an already-dead worker must count as a failure, not a
/// watchdog kill, which would mean master decided to kill a worker it
/// believed alive.
/// A client that goes away mid-script must not cost the pool a worker: the
/// checked-out worker is owned by the connection's own task, and losing its
/// slot lowers `processes.max` for the rest of the process's life.
#[tokio::test]
async fn a_client_that_disconnects_mid_script_leaves_the_pool_at_full_strength() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "disconnect-mid-script",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 2, "spare": 2 } } }),
    )
    .await;

    let before = status_json(&server).await;
    assert_eq!(before["php"]["processes"]["idle"], 2);

    // /slow sleeps well past this, so the connection dies while the script
    // still holds the worker.
    let client = reqwest::Client::new();
    let cut_off = tokio::time::timeout(
        Duration::from_millis(100),
        client
            .get(format!("http://127.0.0.1:{}/slow", server.port))
            .send(),
    )
    .await;
    assert!(cut_off.is_err(), "the request completed instead of being cut off");

    let mut status = serde_json::Value::Null;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        status = status_json(&server).await;
        if status["php"]["processes"]["idle"] == 2 {
            break;
        }
    }
    assert_eq!(
        status["php"]["processes"]["idle"], 2,
        "pool never returned to spare after an abandoned request: {status}"
    );
    assert_eq!(
        status["php"]["counters"]["workers_abandoned"], 1,
        "the disconnect should be accounted for, not silently absorbed: {status}"
    );
    // The reaper is a backstop for deaths master cannot observe; reaching it
    // here would mean the release path missed this one.
    assert_eq!(status["php"]["counters"]["workers_reaped_dead"], 0);
}

#[tokio::test]
async fn worker_killed_mid_stream_ends_the_response_and_pool_recovers() {
    let www = fixtures_dir().join("www");
    // Explicit, not inherited: the fixture's flush() must actually reach the
    // wire for the premise - killing mid-chunk - to hold.
    let server = start_server(
        "worker-killed-mid-stream",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": {
                "processes": { "max": 1, "spare": 1 },
                "options": { "admin": { "output_buffering": "0" } }
            }
        }),
    )
    .await;

    let client = reqwest::Client::new();
    let mut resp = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .get(format!("http://127.0.0.1:{}/stream-then-die", server.port))
            .send(),
    )
    .await
    .expect("headers never arrived")
    .unwrap();
    assert_eq!(resp.status(), 200);
    let first = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
        .await
        .expect("first chunk never arrived")
        .expect("reading the first chunk failed")
        .expect("stream ended before any chunk");
    assert_eq!(&first[..], b"first-chunk\n");

    // `processes.max: 1` - the pid currently in `workers[]` is unambiguously
    // the one serving this request.
    let pids = worker_pids(&server).await;
    assert_eq!(pids.len(), 1, "expected exactly one worker: {pids:?}");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pids[0] as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("failed to SIGKILL the worker");

    // The connection must actually end - not hang forever waiting for
    // bytes that will never come.
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match resp.chunk().await {
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => return,
            }
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "response never ended after its worker was killed - connection hung"
    );

    // Pool must have recovered: a fresh request gets a normal response,
    // not stuck behind a dead worker or a wedged semaphore permit.
    let recovered = client
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .send()
        .await
        .unwrap();
    assert_eq!(
        recovered.status(),
        200,
        "pool did not recover after the worker was killed mid-stream"
    );

    // Already dead by the time master read from it, so this is the
    // read-failure path, not a watchdog kill.
    let status = status_json(&server).await;
    assert!(
        status["php"]["counters"]["requests_failed"]
            .as_u64()
            .unwrap()
            >= 1,
        "got: {status}"
    );
    assert_eq!(
        status["php"]["counters"]["watchdog_kills"]
            .as_u64()
            .unwrap(),
        0,
        "got: {status}"
    );
}

/// Once every worker slot is occupied by a hung request, a further
/// request must not wait indefinitely - it gets 503 once queue.timeout
/// elapses, distinct from the watchdog's 504.
#[tokio::test]
async fn queue_timeout_returns_503_without_waiting_for_watchdog() {
    let stuck = fixtures_dir().join("stuck-www");
    let server = start_server(
        "queue",
        stuck.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 1, "spare": 1 }, "limits": { "timeout": 5 }, "queue": { "timeout": 1 } } }),
    )
    .await;
    let client = reqwest::Client::new();

    // Occupy the single worker slot.
    let occupier = tokio::spawn({
        let client = client.clone();
        let url = format!("http://127.0.0.1:{}/x", server.port);
        async move { client.get(url).send().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let start = tokio::time::Instant::now();
    let resp = client
        .get(format!("http://127.0.0.1:{}/y", server.port))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), 503);
    assert!(
        elapsed < Duration::from_secs(3),
        "503 should arrive at queue.timeout (~1s), not wait for the occupier's 5s watchdog: {elapsed:?}"
    );

    let _ = occupier.await;
}

/// Large + Accept-Encoding: gzip -> compressed; small body -> not.
#[tokio::test]
async fn compression_respects_min_size_threshold() {
    let www = fixtures_dir().join("www");
    let server = start_server("compression", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap(); // decode manually to check the header

    let big = client
        .get(format!("http://127.0.0.1:{}/big.txt", server.port))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(big.headers().get("content-encoding").unwrap(), "gzip");

    let small = client
        .get(format!("http://127.0.0.1:{}/hello.txt", server.port))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert!(small.headers().get("content-encoding").is_none());
}

/// zstd > brotli > gzip priority (explicit product choice), end-to-end -
/// not just the pure `pick_encoding` unit tests, actual wire bytes decoded
/// back to the original content.
#[tokio::test]
async fn compression_prefers_zstd_over_brotli_and_gzip() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "compression-zstd",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap();

    let resp = client
        .get(format!("http://127.0.0.1:{}/big.txt", server.port))
        .header("Accept-Encoding", "gzip, br, zstd")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "zstd");
    let compressed = resp.bytes().await.unwrap();
    let decoded = String::from_utf8(zstd::decode_all(compressed.as_ref()).unwrap()).unwrap();

    let expected = tokio::fs::read_to_string(www.join("public/big.txt"))
        .await
        .unwrap();
    assert_eq!(decoded, expected);
}

/// Status endpoint returns the documented JSON shape.
#[tokio::test]
async fn status_endpoint_reports_pool_shape() {
    let www = fixtures_dir().join("www");
    let server = start_server("status", www.to_str().unwrap(), serde_json::json!({})).await;

    // Exercise a real PHP dispatch first, so `workers[]` has a real,
    // request_count > 0 entry to check, not just the pre-spawned spares.
    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert!(status["php"]["processes"]["max"].as_u64().unwrap() >= 1);
    assert!(status["php"]["counters"]["requests_total"].is_u64());
    assert!(status["uptime_seconds"].is_u64());

    // `start_server`'s base config defines exactly one target, named
    // "default".
    assert_eq!(status["php"]["targets"], serde_json::json!(["default"]));
    assert!(status["php"]["prototype_pid"].as_i64().unwrap() > 0);
    let idle = status["php"]["processes"]["idle"].as_u64().unwrap();
    let busy = status["php"]["processes"]["busy"].as_u64().unwrap();
    assert_eq!(
        status["php"]["processes"]["total"].as_u64().unwrap(),
        idle + busy
    );
    assert!(status["php"]["queue"]["depth"].is_u64());
    assert!(status["php"]["queue"]["max_depth"].is_u64());
    for counter in [
        "requests_failed",
        "recycled_request_limit",
        "recycled_idle_timeout",
        "prototype_respawns_total",
        "crash_loop_backoffs",
    ] {
        assert!(
            status["php"]["counters"][counter].is_u64(),
            "missing counter: {counter}"
        );
    }

    let workers = status["php"]["workers"].as_array().unwrap();
    assert!(
        !workers.is_empty(),
        "expected at least one worker after a real dispatch"
    );
    let served = workers
        .iter()
        .find(|w| w["request_count"].as_u64() == Some(1));
    assert!(
        served.is_some(),
        "expected a worker with request_count == 1, got: {workers:?}"
    );
    let w = served.unwrap();
    assert!(w["pid"].as_i64().unwrap() > 0);
    assert!(w["state"] == "idle" || w["state"] == "busy");
    assert!(w["started_ago_seconds"].is_u64());
    assert!(w["last_active_ago_seconds"].is_u64());
}

/// A live-confirmed traversal: this exact request once returned /etc/passwd.
///
/// Raw TCP, because HTTP client libraries normalise dot segments before the
/// request is sent, so the traversal bytes never reach the server. An
/// attacker is under no such constraint.
#[tokio::test]
async fn static_route_rejects_path_traversal() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server("traversal", www.to_str().unwrap(), serde_json::json!({})).await;

    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    sock.write_all(
        b"GET /../../../../../../etc/passwd HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let mut resp = String::new();
    sock.read_to_string(&mut resp).await.unwrap();

    assert!(
        resp.starts_with("HTTP/1.1 400"),
        "expected 400, got: {resp}"
    );
    assert!(
        !resp.contains("root:"),
        "must never leak /etc/passwd contents: {resp}"
    );
}

/// The real request reaches PHP's `$_SERVER`. The client IP is loopback here
/// so this only proves it arrives; the resolution logic is unit-tested.
#[tokio::test]
async fn php_receives_real_request_data() {
    let www = fixtures_dir().join("www");
    let server = start_server("reqdata", www.to_str().unwrap(), serde_json::json!({})).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/app?x=1", server.port))
        .header("X-Test", "custom-header-value")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("METHOD=GET"), "got: {body}");
    assert!(body.contains("URI=/app?x=1"), "got: {body}");
    assert!(
        body.contains("HEADER_X_TEST=custom-header-value"),
        "got: {body}"
    );
    assert!(body.contains("REMOTE_ADDR=127.0.0.1"), "got: {body}");
}

/// Both spellings collapse to one CGI var under RFC 3875 §4.1.18's
/// hyphen-to-underscore rule, which has no inverse, so a name containing a
/// literal underscore must never arrive at all and shadow a hyphenated one.
#[tokio::test]
async fn header_with_a_literal_underscore_never_reaches_php() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "underscore-header",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .header("X_Test", "should-never-arrive")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("HEADER_X_TEST=MISSING"),
        "an underscore-named header must not populate HTTP_X_TEST: {body}"
    );
}

/// The standard CGI vars, plus `php.environment` reaching `getenv()`. The
/// forwarded-derived values need this client's loopback address to be a
/// configured trusted proxy; nothing is trusted implicitly.
#[tokio::test]
async fn php_receives_server_vars_and_environment() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "servervars",
        www.to_str().unwrap(),
        serde_json::json!({
            "trusted_proxies": ["127.0.0.1/32"],
            "php": { "environment": { "TEST_ENV_VAR": "hello-env" } }
        }),
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .header("Host", "internal-lb:1111")
        .header("X-Forwarded-Host", "example.test:9999")
        .header("X-Forwarded-Proto", "https")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("SERVER_NAME=example.test"), "got: {body}");
    assert!(body.contains("SERVER_PORT=9999"), "got: {body}");
    assert!(body.contains("SERVER_PROTOCOL=HTTP/1.1"), "got: {body}");
    assert!(body.contains("GATEWAY_INTERFACE=CGI/1.1"), "got: {body}");
    assert!(
        body.contains(&format!("DOCUMENT_ROOT={}", www.to_str().unwrap())),
        "got: {body}"
    );
    assert!(
        body.contains(&format!(
            "SCRIPT_FILENAME={}/index.php",
            www.to_str().unwrap()
        )),
        "got: {body}"
    );
    assert!(body.contains("SCRIPT_NAME=/index.php"), "got: {body}");
    assert!(body.contains("PATH_INFO=/app"), "got: {body}");
    assert!(body.contains("PHP_SELF=/index.php/app"), "got: {body}");
    assert!(body.contains("HTTPS=on"), "got: {body}");
    assert!(body.contains("REQUEST_TIME_SET=yes"), "got: {body}");
    assert!(body.contains("ENV_GETENV=hello-env"), "got: {body}");
    // A non-empty `php.environment` must not force 'E' into
    // `variables_order`, which would leak the whole real environment into
    // every script's $_ENV off the back of an unrelated config field.
    assert!(body.contains("ENV_SUPERGLOBAL=MISSING"), "got: {body}");
}

/// The opt-in path: an operator who wants `$_ENV` populated sets
/// `variables_order` themselves, needing no coupling on the server's side.
#[tokio::test]
async fn php_environment_reaches_env_superglobal_when_variables_order_is_set_explicitly() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "servervars-explicit",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": {
                "environment": { "TEST_ENV_VAR": "hello-env" },
                "options": { "admin": { "variables_order": "EGPCS" } }
            }
        }),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("ENV_GETENV=hello-env"), "got: {body}");
    assert!(body.contains("ENV_SUPERGLOBAL=hello-env"), "got: {body}");
}

/// Both take a raw header value through a side channel rather than the
/// generic header-to-$_SERVER path, php_embed otherwise leaving the
/// cookie-parsing hook disabled entirely.
#[tokio::test]
async fn php_receives_cookies_and_basic_auth() {
    let www = fixtures_dir().join("www");
    let server = start_server("cookie-auth", www.to_str().unwrap(), serde_json::json!({})).await;

    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/cookie-and-auth", server.port))
        .header("Cookie", "foo=bar; baz=qux")
        .basic_auth("alice", Some("s3cret"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("COOKIE_FOO=bar"), "got: {body}");
    assert!(body.contains("COOKIE_BAZ=qux"), "got: {body}");
    assert!(body.contains("AUTH_USER=alice"), "got: {body}");
    assert!(body.contains("AUTH_PW=s3cret"), "got: {body}");
}

/// The directive must reach the engine, not merely make `ini_get` report the
/// right string: `function_exists()` is the real signal, and calling it
/// anyway must still fail loudly.
#[tokio::test]
async fn disable_functions_actually_disables_the_function() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "disable-fn",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "options": { "admin": { "disable_functions": "exec" } } } }),
    )
    .await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/disabled-function-check",
        server.port
    ))
    .await
    .unwrap();
    let body = resp.text().await.unwrap();
    assert!(body.contains("EXEC_EXISTS=false"), "got: {body}");

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/call-disabled-function",
        server.port
    ))
    .await
    .unwrap();
    assert_eq!(
        resp.status(),
        500,
        "calling a disabled function must still surface as a real error, not silently no-op"
    );
}

/// Admin wins on a key collision, and an `ini_set()` against it from inside
/// the script must be rejected: the modify type mutates the directive's own
/// modifiable flag, not just its value.
#[tokio::test]
async fn admin_ini_option_wins_over_user_and_locks_against_ini_set() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "admin-ini-lock",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": {
                "options": {
                    "user": { "memory_limit": "64M" },
                    "admin": { "memory_limit": "128M" }
                }
            }
        }),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/ini-check", server.port))
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("MEMORY_LIMIT=128M"),
        "admin should win over user on the same key: {body}"
    );
    assert!(
        body.contains("INI_SET_RESULT=false"),
        "an admin-locked directive must reject the script's own ini_set(): {body}"
    );
    assert!(
        body.contains("MEMORY_LIMIT_AFTER_SET=128M"),
        "the rejected ini_set() must not have changed anything: {body}"
    );
}

/// This client is always a loopback peer, so only the value is exercised
/// here; the trust boundary itself needs an untrusted peer and is unit-tested.
#[tokio::test]
async fn https_off_by_default_without_the_forwarded_header() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "https-default",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("HTTPS=MISSING"),
        "no X-Forwarded-Proto sent, HTTPS must stay absent: {body}"
    );
}

/// The response must return well before the script's own post-response
/// sleep, proven by a marker file written only after it, and the worker must
/// still return to the idle pool rather than stay busy forever.
#[tokio::test]
async fn fastcgi_finish_request_responds_early_and_keeps_worker_running() {
    let www = fixtures_dir().join("www");
    let server = start_server("fcgifinish", www.to_str().unwrap(), serde_json::json!({})).await;

    let marker = format!("/tmp/fastcgi_finish_marker_{}.txt", server.port);
    let _ = std::fs::remove_file(&marker);

    let start = std::time::Instant::now();
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/fastcgi-finish?marker={}",
        server.port, server.port
    ))
    .await
    .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.starts_with("quick-response"), "got: {body}");
    assert!(
        elapsed < Duration::from_millis(150),
        "response took {elapsed:?} - fastcgi_finish_request() should have returned it \
         immediately, well before the script's own 300ms post-response sleep"
    );

    // Background work genuinely still running right after the response -
    // proves finish_request delivered the response WITHOUT waiting for it.
    assert!(
        !std::path::Path::new(&marker).exists(),
        "background work finished suspiciously fast"
    );

    // ...but it does eventually finish, and the worker comes back to idle.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if std::path::Path::new(&marker).exists() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "background work never completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let marker_body = std::fs::read_to_string(&marker).unwrap();
    assert!(
        marker_body.contains("background-work-done:true"),
        "got: {marker_body}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let status: serde_json::Value =
            reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        if status["php"]["processes"]["busy"].as_u64() == Some(0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker never returned to the idle pool after finishing its background work"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let _ = std::fs::remove_file(&marker);
}

/// After `End` the worker may still be running its script, so the worker and
/// its permit stay claimed until the done marker arrives - and a client that
/// hangs up in that window must not shorten it. Otherwise the next request
/// is handed a worker still executing the previous one.
#[tokio::test]
async fn a_client_hanging_up_after_an_early_response_still_waits_out_the_worker() {
    use std::io::Read;

    let www = fixtures_dir().join("www");
    let server = start_server(
        "fcgifinish-hangup",
        www.to_str().unwrap(),
        // One worker, so one permit: a second request can only be served
        // once this one's whole lifecycle is finished with it.
        serde_json::json!({ "php": { "processes": { "max": 1, "spare": 1 } } }),
    )
    .await;

    let marker = format!("/tmp/fastcgi_finish_marker_{}.txt", server.port);
    let _ = std::fs::remove_file(&marker);

    let before: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let worker_pid = before["php"]["workers"][0]["pid"].as_u64().unwrap();

    let started = std::time::Instant::now();
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        sock,
        "GET /fastcgi-finish?marker={} HTTP/1.1\r\nHost: localhost\r\n\r\n",
        server.port
    )
    .unwrap();

    // Only the headers: hanging up without ever reading the body is the
    // abort this is about.
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        assert!(head.len() < 8192, "no end of headers in sight");
        assert_eq!(sock.read(&mut byte).unwrap(), 1, "connection closed early");
        head.push(byte[0]);
    }
    let answered = started.elapsed();
    sock.shutdown(std::net::Shutdown::Both).unwrap();
    drop(sock);

    let head = String::from_utf8_lossy(&head).to_string();
    assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
    assert!(
        answered < Duration::from_millis(150),
        "the response should arrive well before the script's 300ms post-response sleep, \
         took {answered:?}"
    );
    assert!(
        !std::path::Path::new(&marker).exists(),
        "the script's background work was already over - this raced nothing"
    );

    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        status["php"]["processes"]["busy"].as_u64(),
        Some(1),
        "the worker was pooled while its script was still running: {status}"
    );

    let marker_body = loop {
        if let Ok(body) = std::fs::read_to_string(&marker) {
            break body;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the abandoned script's background work never completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        marker_body.contains("background-work-done:true"),
        "got: {marker_body}"
    );
    let _ = std::fs::remove_file(&marker);

    // Same pid, so the hang-up recycled the worker through its lifecycle
    // rather than killing it and hiding that behind a fresh spawn.
    let status = loop {
        let status: serde_json::Value =
            reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        if status["php"]["processes"]["busy"].as_u64() == Some(0) {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the worker never came back to the pool: {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(status["php"]["processes"]["idle"].as_u64(), Some(1));
    assert_eq!(
        status["php"]["workers"][0]["pid"].as_u64(),
        Some(worker_pid),
        "the worker was replaced, not reused: {status}"
    );
    let counters = &status["php"]["counters"];
    assert_eq!(counters["watchdog_kills"].as_u64(), Some(0), "{counters}");
    assert_eq!(counters["requests_failed"].as_u64(), Some(0), "{counters}");

    // And it still serves, on the very worker the vanished client left behind.
    let resp = reqwest::get(format!("http://127.0.0.1:{}/", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Registering the function without an owning module crashes the Optimizer's
/// `function_exists()` constant-folding pass, but only when compiling a class
/// loaded via autoload rather than the primary script.
///
/// Every other fixture here is a flat script, so this is the only test that
/// reaches the trigger condition at all.
#[tokio::test]
async fn fastcgi_finish_request_registration_survives_autoloaded_compilation() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "fcgifinish-autoload",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/fastcgi-finish-autoload",
        server.port
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "has_fastcgi_finish_request=true\n", "got: {body}");
}

/// An early response takes the same compression path as a normal one, the
/// worker sending the same shape either way, just sooner.
#[tokio::test]
async fn fastcgi_finish_request_response_is_still_compressed() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "fcgifinishgzip",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap(); // inspect the raw header/body ourselves

    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/fastcgi-finish?big=1",
            server.port
        ))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").unwrap(),
        "gzip",
        "early response bodies over the compression threshold must still get gzipped"
    );
    assert_eq!(
        resp.headers().get("vary").unwrap(),
        "Accept-Encoding",
        "a PHP response that can end up compressed must vary by Accept-Encoding, same as a static file"
    );
    let compressed = resp.bytes().await.unwrap();
    let mut decoded = String::new();
    std::io::Read::read_to_string(
        &mut flate2::read::GzDecoder::new(compressed.as_ref()),
        &mut decoded,
    )
    .unwrap();
    assert!(
        decoded.starts_with(&"x".repeat(2048)),
        "got: {} bytes decoded",
        decoded.len()
    );
}

/// A script that compressed the body itself must not be compressed again.
/// Its `Content-Encoding` is truthful, so stripping it is no fix either: that
/// would leave gzip bytes labelled as plain.
#[tokio::test]
async fn a_script_that_encoded_its_own_body_is_not_encoded_again() {
    let www = fixtures_dir().join("www");
    let server = start_server("pre-encoded", www.to_str().unwrap(), serde_json::json!({})).await;

    // Decoding nothing itself, so the wire bytes and every header are visible.
    let client = reqwest::Client::builder().no_gzip().build().unwrap();
    let resp = client
        .get(format!("http://127.0.0.1:{}/pre-encoded", server.port))
        .header("accept-encoding", "zstd, br, gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let encodings: Vec<String> = resp
        .headers()
        .get_all("content-encoding")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert_eq!(
        encodings,
        vec!["gzip"],
        "the script's own encoding must survive, exactly once"
    );

    let body = resp.bytes().await.unwrap();
    assert_eq!(
        &body[..2],
        b"\x1f\x8b",
        "body is no longer gzip - it was encoded a second time"
    );
}

/// A PHP stream has no known length to gate on, so eligibility comes from
/// Content-Type alone and even a tiny response is compressed and varies.
#[tokio::test]
async fn small_php_response_still_gets_compressed_and_varies() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "fcgifinish-small-vary",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap();

    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/fastcgi-finish?marker=novary",
            server.port
        ))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").unwrap(),
        "gzip",
        "mime-eligible responses compress regardless of size"
    );
    assert_eq!(resp.headers().get("vary").unwrap(), "Accept-Encoding");
    let compressed = resp.bytes().await.unwrap();
    let mut decoded = String::new();
    std::io::Read::read_to_string(
        &mut flate2::read::GzDecoder::new(compressed.as_ref()),
        &mut decoded,
    )
    .unwrap();
    assert!(decoded.starts_with("quick-response pid="), "got: {decoded}");
}

/// `spare > max` must fail at startup with a clear reason rather than
/// misbehave at runtime.
#[tokio::test]
async fn invalid_config_fails_fast_instead_of_starting() {
    let config = serde_json::json!({
        "listen": ["127.0.0.1:0"],
        "status": { "listen": "127.0.0.1:0" },
        "routes": [],
        "php": {
            "user": "phpapp",
            "group": "phpapp",
            "limits": { "requests": 3, "timeout": 2 },
            "processes": { "max": 1, "spare": 5 },
            "queue": { "timeout": 2 }
        }
    });
    let config_path = std::env::temp_dir().join("test-config-invalid.json");
    std::fs::File::create(&config_path)
        .unwrap()
        .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_proteus"))
            .arg(&config_path)
            .output()
    })
    .await
    .unwrap()
    .unwrap();

    assert!(
        !output.status.success(),
        "server should refuse to start with php.processes.spare > max"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("php.processes.spare"),
        "got stderr: {stderr}"
    );
}

/// A direct invocation, which nothing stops a human from attempting, must
/// fail cleanly and immediately rather than panic on an unrelated fd error
/// or hang.
#[tokio::test]
async fn internal_prototype_flag_refuses_a_direct_invocation() {
    let output = tokio::task::spawn_blocking(|| {
        Command::new(env!("CARGO_BIN_EXE_proteus"))
            .arg("--internal-prototype")
            .output()
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        output.status.code(),
        Some(1),
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not a SOCK_SEQPACKET control socket"),
        "got stderr: {stderr}"
    );
}

/// Self-contained, since the shared harness discards stdout. Drains the pipe
/// on a background thread: `Child`'s stdout is blocking, and leaving it
/// unread deadlocks the child once the pipe buffer fills.
#[tokio::test]
async fn access_log_includes_worker_pid_and_php_target() {
    let www = fixtures_dir().join("www");
    let port = next_port();
    let status_port = next_port();
    let config = serde_json::json!({
        "listen": [format!("127.0.0.1:{port}")],
        "status": { "listen": format!("127.0.0.1:{status_port}") },
        "routes": [
            { "match": {}, "action": "static",
              "root": format!("{}/public", www.to_str().unwrap()),
              "fallback": { "action": "php", "target": "default" } }
        ],
        "php": {
            "targets": { "default": { "root": www.to_str().unwrap(), "script": "index.php" } },
            "user": "phpapp",
            "group": "phpapp",
            "limits": { "requests": 3, "timeout": 2 },
            "processes": { "max": 4, "spare": 2 },
            "queue": { "timeout": 2 }
        }
    });
    let config_path = std::env::temp_dir().join("test-config-accesslog.json");
    std::fs::File::create(&config_path)
        .unwrap()
        .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_proteus"))
        .arg(&config_path)
        .env("PROTEUS_PHP_MOD_PATH", php_mod_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server binary");
    let stdout = child.stdout.take().unwrap();
    let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let lines_writer = std::sync::Arc::clone(&lines);
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            lines_writer.lock().unwrap().push(line);
        }
    });
    let _guard = ChildGuard(&mut child);

    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("server never became ready");
        }
        match client
            .get(format!("http://127.0.0.1:{status_port}/"))
            .timeout(Duration::from_millis(500))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => break,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }

    let resp = client
        .get(format!("http://127.0.0.1:{port}/app"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let found = lines.lock().unwrap().iter().find_map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).ok()?;
            (v.get("action")?.as_str()? == "php").then_some(v)
        });
        if let Some(entry) = found {
            assert!(entry["worker_pid"].as_u64().unwrap() > 0, "got: {entry}");
            assert_eq!(
                entry["php_target"].as_str().unwrap(),
                "default",
                "got: {entry}"
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "access log line for the php request never appeared"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Past the depth cap, the next request must be rejected immediately rather
/// than after waiting out the queue timeout.
#[tokio::test]
async fn queue_max_depth_rejects_immediately_when_full() {
    let stuck = fixtures_dir().join("stuck-www");
    let server = start_server(
        "maxdepth",
        stuck.to_str().unwrap(),
        serde_json::json!({
            "php": {
                "processes": { "max": 1, "spare": 1 },
                "limits": { "timeout": 30 },
                "queue": { "timeout": 5, "max_depth": 1 }
            }
        }),
    )
    .await;
    let client = reqwest::Client::new();

    // Occupy the single worker slot - stuck-www's script hangs forever.
    let _occupier = tokio::spawn({
        let client = client.clone();
        let url = format!("http://127.0.0.1:{}/x", server.port);
        async move { client.get(url).send().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Fills the one allowed queue slot (max_depth=1) - genuinely waiting
    // for a permit, not rejected, since nothing else is queued yet.
    let _queued = tokio::spawn({
        let client = client.clone();
        let url = format!("http://127.0.0.1:{}/y", server.port);
        async move { client.get(url).send().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Queue is now full - this one must be rejected right away, not after
    // waiting out queue.timeout (5s).
    let start = tokio::time::Instant::now();
    let resp = client
        .get(format!("http://127.0.0.1:{}/z", server.port))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(resp.status(), 503);
    assert!(
        elapsed < Duration::from_secs(1),
        "should reject immediately, took {elapsed:?}"
    );
}

/// POST body reaches PHP via php://input (read_post callback).
#[tokio::test]
async fn php_receives_post_body() {
    let www = fixtures_dir().join("www");
    let server = start_server("postbody", www.to_str().unwrap(), serde_json::json!({})).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/submit", server.port))
        .body("hello-from-integration-test")
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("METHOD=POST"), "got: {body}");
    assert!(
        body.contains("BODY=hello-from-integration-test"),
        "got: {body}"
    );
}

/// Not merely that PHP saw bytes: `is_uploaded_file()` and
/// `move_uploaded_file()` succeed only if PHP's own rfc1867 handler ran, which
/// php_embed's default server context silently disables rather than erroring.
#[tokio::test]
async fn php_receives_multipart_file_upload() {
    let www = fixtures_dir().join("www");
    let server = start_server("upload", www.to_str().unwrap(), serde_json::json!({})).await;

    let file_content = b"hello from an uploaded file\n".to_vec();
    let part = reqwest::multipart::Part::bytes(file_content.clone())
        .file_name("greeting.txt")
        .mime_str("text/plain")
        .unwrap();
    let form = reqwest::multipart::Form::new()
        .part("upload", part)
        .text("note", "a-regular-field");

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/upload", server.port))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    assert!(body.contains("NAME=greeting.txt"), "got: {body}");
    assert!(
        body.contains(&format!("SIZE={}", file_content.len())),
        "got: {body}"
    );
    assert!(body.contains("ERROR=0"), "got: {body}");
    assert!(body.contains("IS_UPLOADED_FILE=true"), "got: {body}");
    assert!(body.contains("MOVE_UPLOADED_FILE=true"), "got: {body}");
    assert!(
        body.contains("MOVED_CONTENT=hello from an uploaded file"),
        "got: {body}"
    );
    assert!(body.contains("FIELD=a-regular-field"), "got: {body}");
}

/// A large request body spills to a temp file rather than accumulating in
/// memory, and the worker reads it back from there. The bytes must round-trip
/// exactly, and the temp file must not linger once the request finishes.
#[tokio::test]
async fn php_receives_a_large_spilled_request_body_correctly() {
    let www = fixtures_dir().join("www");
    let server = start_server("upload-spill", www.to_str().unwrap(), serde_json::json!({})).await;

    // Comfortably over the 256KiB in-memory threshold, comfortably under
    // the 64MiB default max_body_size - exercises the spill path without
    // a slow upload.
    let body: Vec<u8> = (0..(600 * 1024usize)).map(|i| (i % 256) as u8).collect();
    let before = list_spilled_body_files();

    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/echo-body", server.port))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let echoed = resp.bytes().await.unwrap();
    assert_eq!(echoed.as_ref(), body.as_slice());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let now = list_spilled_body_files();
        let leftover: Vec<_> = now.difference(&before).collect();
        if leftover.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "spilled request body temp file was never cleaned up: {leftover:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The spill path is fully predictable, so creating it without `O_EXCL` would
/// follow a pre-planted symlink and land request-body bytes in any file master
/// can write. A file a planted symlink points at must come back untouched.
///
/// A blocked spillover must fail the whole request (500), not silently
/// dispatch it to PHP as an empty body: a request whose body could not be
/// prepared must never look like a valid, merely-empty one.
#[tokio::test]
async fn spilled_body_file_creation_refuses_to_follow_a_preplanted_symlink() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "body-symlink-attack",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let canary = std::env::temp_dir().join(format!("symlink-attack-canary-{}", std::process::id()));
    std::fs::write(&canary, b"untouched").expect("failed to create canary file");

    // The exact path `temp_body_path()` will use for this fresh server's
    // FIRST spillover - `BODY_FILE_COUNTER` starts at 0 in every new
    // process, so this is predictable by construction, not a guess.
    let predicted_path = std::env::temp_dir().join(format!("proteus-body-{}-0", server.child.id()));
    std::os::unix::fs::symlink(&canary, &predicted_path).expect("failed to plant symlink");

    let body: Vec<u8> = vec![0x41; 600 * 1024]; // over BODY_MEMORY_THRESHOLD, triggers spillover
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/submit", server.port))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        500,
        "a blocked spillover must fail the request, not dispatch it as an empty body"
    );
    let resp_body = resp.text().await.unwrap();
    assert!(
        resp_body.contains("500 Internal Server Error"),
        "must be the server's own synthesized error, not anything PHP-generated: got {resp_body:?}"
    );

    let canary_contents = std::fs::read(&canary).expect("canary file should still exist");
    assert_eq!(
        canary_contents, b"untouched",
        "symlink must not have been followed and written through"
    );
    assert!(
        std::fs::symlink_metadata(&predicted_path)
            .unwrap()
            .file_type()
            .is_symlink(),
        "the pre-planted symlink itself must still be exactly that, not replaced by a real file"
    );

    let _ = std::fs::remove_file(&predicted_path);
    let _ = std::fs::remove_file(&canary);
}

/// A client that closes its write side mid-body is a real I/O error, not a
/// stall (distinct from `body_read_timeout`) - must fail the request rather
/// than dispatch it to PHP looking like a normal, merely short, body.
#[tokio::test]
async fn a_request_body_that_disconnects_mid_transfer_fails_the_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "body-disconnect",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    stream
        .write_all(b"POST /submit HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\n\r\n")
        .await
        .unwrap();
    stream.write_all(b"short").await.unwrap();
    // Half-close: the declared 1000 bytes will now never arrive, but the
    // read side stays open so the server's response can still be read back.
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
        .await
        .expect("server never responded to the truncated body")
        .unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 500"), "got: {response}");
    assert!(
        response.contains("500 Internal Server Error"),
        "must be the server's own synthesized error, not anything PHP-generated: got {response}"
    );
}

fn list_spilled_body_files() -> std::collections::HashSet<std::path::PathBuf> {
    std::fs::read_dir(std::env::temp_dir())
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("proteus-body-"))
        })
        .collect()
}

/// The client sees PHP's own http_response_code(), not always a
/// hardcoded 200 regardless of what the script did.
#[tokio::test]
async fn php_response_status_code_reaches_client() {
    let www = fixtures_dir().join("www");
    let server = start_server("statuscode", www.to_str().unwrap(), serde_json::json!({})).await;

    let ok = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    let not_found = reqwest::get(format!("http://127.0.0.1:{}/not-found", server.port))
        .await
        .unwrap();
    assert_eq!(not_found.status(), 404);
}

/// Pins real PHP behaviour rather than an ideal: core promotes an uncaught
/// fatal error to 500 only while the response code is still exactly 200, its
/// own sentinel for nothing having been set, and otherwise leaves it alone.
///
/// This depends on the per-request reset: without it a reused worker still
/// read 0 here and dodged that check by accident.
#[tokio::test]
async fn uncaught_error_forces_a_500() {
    let www = fixtures_dir().join("www");
    let server = start_server("fatalerror", www.to_str().unwrap(), serde_json::json!({})).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/fatal-error", server.port))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        500,
        "PHP core promotes an uncaught fatal error from the still-200 default to 500"
    );
}

/// SIGTERM must drain, not just die: an in-flight request has to get
/// its real response, not a cut-off connection, and the process must then
/// actually exit on its own within its configured grace period.
/// Accepting is raced against shutdown, but so is the wait for a connection
/// permit: with every permit held by a live keep-alive, that wait is unbounded,
/// and an accept loop stuck in it never releases what shutdown is waiting on.
#[tokio::test]
async fn sigterm_exits_with_every_connection_permit_taken() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let www = fixtures_dir().join("www");
    let mut server = start_server(
        "sigterm-permits-taken",
        www.to_str().unwrap(),
        serde_json::json!({
            // One permit, and long enough that the holder cannot time out and
            // hand it back on its own.
            "connection": { "max": 1, "idle_timeout": 3600 },
            "php": { "shutdown": { "grace_period_seconds": 3 } }
        }),
    )
    .await;

    // Holds the only permit: served to completion, then kept open and idle, so
    // the drain has nothing of its own left to wait for.
    let mut held = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect");
    held.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .expect("write");
    let mut head = [0u8; 12];
    tokio::time::timeout(Duration::from_secs(5), held.read_exact(&mut head))
        .await
        .expect("no response on the first connection")
        .expect("read");

    // Accepted, then parked waiting for the permit the first one holds.
    let mut waiting = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect");
    waiting
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .expect("write");
    let mut byte = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(500), waiting.read_exact(&mut byte))
            .await
            .is_err(),
        "the second connection was served, so the permit was not held"
    );

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(server.child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to send SIGTERM");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(_)) = server.child.try_wait() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "master never exited: an accept loop is still waiting for a permit"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn sigterm_drains_in_flight_request_before_exiting() {
    let www = fixtures_dir().join("www");
    let mut server = start_server(
        "sigterm",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "shutdown": { "grace_period_seconds": 5 } } }),
    )
    .await;

    let port = server.port;
    let slow_request =
        tokio::spawn(async move { reqwest::get(format!("http://127.0.0.1:{port}/slow")).await });

    // Give the request time to actually be dispatched to a worker before
    // signaling - otherwise this could race and send SIGTERM before the
    // listener even accepted the connection.
    tokio::time::sleep(Duration::from_millis(100)).await;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(server.child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to send SIGTERM");

    let resp = slow_request
        .await
        .unwrap()
        .expect("request must still complete, not be cut off");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("slow done"), "got: {body}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(_)) = server.child.try_wait() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "master never exited after SIGTERM + drain"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The in-flight guard must live until the streamed body finishes, not until
/// the handler returns. SIGTERM lands after the handler has returned but well
/// before the script is done; counting that as idle would let the drain exit
/// instantly and truncate the response mid-stream.
#[tokio::test]
async fn sigterm_drains_an_in_progress_streamed_response_before_exiting() {
    let www = fixtures_dir().join("www");
    let mut server = start_server(
        "sigterm-stream",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "shutdown": { "grace_period_seconds": 5 } } }),
    )
    .await;

    let port = server.port;
    let request = tokio::spawn(async move {
        reqwest::get(format!("http://127.0.0.1:{port}/slow-stream"))
            .await
            .and_then(|r| r.error_for_status())
    });

    // Past the first chunk (headers+first frame arrive near-instantly) but
    // well before the ~1s-total stream naturally finishes - the exact
    // window where handle() has already returned but the body has not.
    tokio::time::sleep(Duration::from_millis(300)).await;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(server.child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("failed to send SIGTERM");

    let resp = request
        .await
        .unwrap()
        .expect("streamed response must still complete, not be cut off");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let expected: String = (0..5).map(|i| format!("chunk-{i}\n")).collect();
    assert_eq!(
        body, expected,
        "response was truncated - in-flight tracking let master exit mid-stream"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(_)) = server.child.try_wait() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "master never exited after SIGTERM + drain"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A dead prototype must degrade individual dispatches to a clean 500, never
/// take the whole master process down with it.
#[tokio::test]
async fn prototype_death_triggers_auto_respawn_without_crashing_master() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "protocrash",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": { "limits": { "requests": 1 }, "processes": { "max": 1, "spare": 1 } }
        }),
    )
    .await;

    // The server's own status field, not /proc: the fork can happen on any
    // runtime thread, and /proc's per-thread children listing reflects only
    // whichever one did it.
    let status_url = format!("http://127.0.0.1:{}/", server.status_port);
    let read_prototype_pid = || {
        let status_url = status_url.clone();
        async move {
            reqwest::get(&status_url)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()["php"]["prototype_pid"]
                .as_i64()
                .unwrap() as i32
        }
    };
    let original_prototype_pid = read_prototype_pid().await;

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(original_prototype_pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("failed to SIGKILL the prototype");

    // Still served by the already-forked spare worker - its data-channel
    // fd was handed to master directly (SCM_RIGHTS) independent of the
    // (now dead) prototype.
    let first = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(first.status(), 200);

    // Retiring that worker forces a fresh spawn, which needs the dead
    // prototype. This must respawn and succeed, not fail cleanly.
    let second = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        200,
        "should succeed via the auto-respawned prototype, not degrade to 500"
    );

    // A different pid, not the original having survived SIGKILL. Fetching
    // this at all also proves master itself did not crash.
    let new_prototype_pid = read_prototype_pid().await;
    assert_ne!(
        new_prototype_pid, original_prototype_pid,
        "prototype pid should have changed after respawn"
    );

    let status: serde_json::Value = reqwest::get(&status_url)
        .await
        .expect("master process appears to have crashed")
        .json()
        .await
        .unwrap();
    assert_eq!(
        status["php"]["counters"]["prototype_respawns_total"].as_u64(),
        Some(1)
    );
}

/// A second death right behind the first must be counted as a backoff rather
/// than acted on, or a crash-looping prototype fork+execs in a tight loop.
#[tokio::test]
async fn repeated_prototype_death_within_the_backoff_window_counts_a_crash_loop_backoff() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "protocrashloop",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": { "limits": { "requests": 1 }, "processes": { "max": 1, "spare": 1 } }
        }),
    )
    .await;

    let status_url = format!("http://127.0.0.1:{}/", server.status_port);
    let read_status = || {
        let status_url = status_url.clone();
        async move {
            reqwest::get(&status_url)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    let kill_prototype = |pid: i64| {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        )
        .expect("failed to SIGKILL the prototype");
    };

    let original_pid = read_status().await["php"]["prototype_pid"]
        .as_i64()
        .unwrap();
    kill_prototype(original_pid);

    // Served by the already-forked spare, which then retires; the dead
    // prototype is not noticed by this request at all.
    let first = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(first.status(), 200);

    // Second request: the retired worker is gone, so this one needs a
    // fresh spawn - which notices the dead prototype and respawns it
    // before retrying, same reactive path as that test's own `second`.
    let second = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        200,
        "should succeed via the auto-respawned prototype"
    );

    // Immediately - well within the 1s backoff window - kill the freshly
    // respawned prototype too, then force another fresh spawn right away
    // (third request, same reasoning as the second).
    let respawned_pid = read_status().await["php"]["prototype_pid"]
        .as_i64()
        .unwrap();
    assert_ne!(
        respawned_pid, original_pid,
        "the second request should have triggered a real respawn"
    );
    kill_prototype(respawned_pid);

    let third = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(
        third.status(),
        500,
        "a respawn attempt inside the backoff window must be rejected, not silently retried until it succeeds"
    );

    let status = read_status().await;
    assert_eq!(
        status["php"]["counters"]["prototype_respawns_total"].as_u64(),
        Some(1),
        "only the first death should have respawned"
    );
    assert!(
        status["php"]["counters"]["crash_loop_backoffs"]
            .as_u64()
            .unwrap()
            >= 1,
        "the second, too-soon death must be counted as a crash-loop backoff: {status}"
    );
}

/// The gap the reactive path cannot cover: with enough spare workers, no
/// spawn is ever needed and nothing would notice the prototype died. Only the
/// background liveness poll catches it, with no request involved.
#[tokio::test]
async fn prototype_death_is_noticed_proactively_even_with_enough_idle_workers() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "protoproactive",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 2, "spare": 2 } } }),
    )
    .await;

    let status_url = format!("http://127.0.0.1:{}/", server.status_port);
    let read_prototype_pid = || {
        let status_url = status_url.clone();
        async move {
            reqwest::get(&status_url)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()["php"]["prototype_pid"]
                .as_i64()
                .unwrap() as i32
        }
    };
    let original_prototype_pid = read_prototype_pid().await;

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(original_prototype_pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("failed to SIGKILL the prototype");

    // Deliberately NOT making any requests here - the two spare workers
    // could serve traffic forever without ever calling spawn_worker. Only
    // the background watch_prototype_liveness loop can notice this.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let pid = read_prototype_pid().await;
        if pid != original_prototype_pid {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "prototype was never proactively respawned despite having no worker-spawn attempt to trigger it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Still fully functional afterward, served by whichever generation of
    // spare worker was already up.
    let resp = reqwest::get(format!("http://127.0.0.1:{}/app", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Idle workers above `spare` get proactively retired (RETIRE
/// protocol) once they've been idle past `idle_timeout`, shrinking the
/// pool back down without needing another request to trigger it.
#[tokio::test]
async fn idle_timeout_retires_workers_above_spare() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "idletimeout",
        www.to_str().unwrap(),
        serde_json::json!({
            "php": {
                "processes": { "max": 4, "spare": 1, "idle_timeout": 1 }
            }
        }),
    )
    .await;

    let client = reqwest::Client::new();
    let port = server.port;
    // 3 concurrent requests -> up to 3 workers spawned (the 1 pre-spawned
    // spare + 2 fresh ones), all back in idle afterward - above spare(1).
    let requests: Vec<_> = (0..3)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .get(format!("http://127.0.0.1:{port}/app"))
                    .send()
                    .await
            })
        })
        .collect();
    for r in requests {
        assert_eq!(r.await.unwrap().unwrap().status(), 200);
    }

    let status_url = format!("http://127.0.0.1:{}/", server.status_port);
    let read_idle = || {
        let status_url = status_url.clone();
        async move {
            reqwest::get(&status_url)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()["php"]["processes"]["idle"]
                .as_u64()
                .unwrap()
        }
    };

    let idle_now = read_idle().await;
    assert!(
        idle_now > 1,
        "expected more than spare(1) idle workers right after 3 concurrent requests, got {idle_now}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        let idle = read_idle().await;
        if idle <= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle pool never shrank back to spare(1), still {idle}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Nothing tells a parked worker its prototype is gone: `peer_death` is set
/// only on the worker's own exit, so it would survive reparented to init,
/// holding a whole PHP heap forever. `PR_SET_PDEATHSIG` is the guard.
///
/// SIGKILL leaves no chance to notify anyone, so anything surviving did so on
/// its own. Fresh workers appearing afterwards is expected; the assertion is
/// about the original instances, fingerprinted by start-time.
#[tokio::test]
async fn workers_do_not_outlive_a_killed_prototype() {
    let www = fixtures_dir().join("www");
    let server = start_server("pdeathsig", www.to_str().unwrap(), serde_json::json!({})).await;

    let status = status_json(&server).await;
    let prototype_pid = status["php"]["prototype_pid"]
        .as_i64()
        .expect("status must report a prototype pid");
    let workers_before = worker_pids(&server).await;
    assert!(
        !workers_before.is_empty(),
        "expected pre-spawned spare workers to inherit the guard"
    );

    let start_times_before: std::collections::HashMap<i64, String> = workers_before
        .iter()
        .map(|&p| {
            (
                p,
                process_start_time(p).expect("pre-existing worker must be readable"),
            )
        })
        .collect();

    // SIGKILL, not SIGTERM: an orderly shutdown could plausibly tear the
    // workers down some other way, which would let a missing PDEATHSIG
    // pass. This leaves the kernel as the only thing that could.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(prototype_pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("failed to kill the prototype");

    // A leaked worker never dies however long this polls, so a generous
    // deadline only costs time in the genuinely-broken case. Same
    // parallelism caveat as the sibling leak test above.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for pid in workers_before {
        let expected_start = start_times_before[&pid].clone();
        loop {
            // Start-time mismatch (or disappearance) is what proves the
            // original instance is gone; a bare pid check would be
            // satisfied by an unrelated process reusing the number.
            if process_start_time(pid).as_deref() != Some(expected_start.as_str()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker pid={pid} (start_time={expected_start}) outlived the prototype that forked it - it is \
                 parked on a ring with nobody left to write to it. /proc/{pid}/wchan: {}",
                std::fs::read_to_string(format!("/proc/{pid}/wchan")).unwrap_or_default(),
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // The pool has to actually come back, not just shed the old workers -
    // otherwise "everything died" would pass this test on its own.
    let recovery_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let resp = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/", server.port))
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        if matches!(&resp, Ok(r) if r.status().is_success()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "master never recovered after the prototype was killed: {resp:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A script that declares its own `Content-Length` gets the minimum-size gate
/// applied, with no buffering to find the size out.
///
/// The header itself must still never reach the client, master always
/// computing framing from the real body.
#[tokio::test]
async fn a_script_declared_content_length_gates_compression_without_being_forwarded() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "declared-length-gate",
        www.to_str().unwrap(),
        // Explicit rather than relying on the default, so the test states
        // the threshold it straddles.
        serde_json::json!({ "compression": { "min_size_bytes": 1024 } }),
    )
    .await;
    let client = reqwest::Client::builder().no_gzip().build().unwrap();

    // Below the threshold: not compressed, and no Vary either - no
    // Accept-Encoding could have changed this response.
    let small = client
        .get(format!(
            "http://127.0.0.1:{}/declared-length?size=100",
            server.port
        ))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(small.status(), 200);
    assert!(
        small.headers().get("content-encoding").is_none(),
        "a script-declared 100 bytes is under min_size_bytes and must not be compressed"
    );
    assert!(
        small.headers().get("vary").is_none(),
        "an ineligible response must not advertise Vary"
    );
    assert!(
        small.headers().get("content-length").is_none()
            || small.headers().get("content-length").unwrap() == "100",
        "if a Content-Length is sent it must be master's own count, never the script's header passed through"
    );
    assert_eq!(small.text().await.unwrap().len(), 100);

    // Above it: compressed exactly as before.
    let big = client
        .get(format!(
            "http://127.0.0.1:{}/declared-length?size=4096",
            server.port
        ))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(big.status(), 200);
    assert_eq!(
        big.headers().get("content-encoding").unwrap(),
        "gzip",
        "a script-declared 4096 bytes is over min_size_bytes and must still compress"
    );
    assert_eq!(big.headers().get("vary").unwrap(), "Accept-Encoding");
    let compressed = big.bytes().await.unwrap();
    let mut decoded = String::new();
    std::io::Read::read_to_string(
        &mut flate2::read::GzDecoder::new(compressed.as_ref()),
        &mut decoded,
    )
    .unwrap();
    assert_eq!(
        decoded.len(),
        4096,
        "the body must survive the round trip intact"
    );
}

/// `queue.timeout` bounds only acquiring a permit; everything after it had no
/// deadline, and it holds the control mutex while waiting, so one wedged
/// prototype stalls the whole pool with no 503, no 504, and nothing moving.
///
/// `SIGSTOP` is the sharp version of wedged: alive with its socket open, so
/// nothing short of a real timeout can notice. A killed prototype is
/// detectable from EOF alone and covered elsewhere.
#[tokio::test]
async fn a_wedged_prototype_does_not_hang_dispatch_forever() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "wedged-prototype",
        www.to_str().unwrap(),
        // No spares, so the first request must ask the prototype. The queue
        // timeout sits well above the spawn one, so the rescue provably comes
        // from the spawn deadline rather than queueing giving up.
        serde_json::json!({
            "php": {
                "processes": { "max": 2, "spare": 0, "spawn_timeout": 2 },
                "queue": { "timeout": 30 }
            }
        }),
    )
    .await;

    let prototype_pid = status_json(&server).await["php"]["prototype_pid"]
        .as_i64()
        .expect("a prototype pid");
    let start_time_before = process_start_time(prototype_pid).expect("prototype must be readable");

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(prototype_pid as i32),
        nix::sys::signal::Signal::SIGSTOP,
    )
    .expect("failed to stop the prototype");

    // The assertion is the deadline itself: without a spawn timeout this
    // request never returns at all. 20s is far above spawn_timeout (2s)
    // plus a respawn, and far below "forever".
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/", server.port))
            .send(),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        result.is_ok(),
        "a wedged prototype hung this dispatch for over 20s - the spawn deadline never fired"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "request took {elapsed:?}, which is not a bounded failure"
    );

    // Replacing a `Child` does not kill what it refers to, so a respawn that
    // only swapped the handle would leave this one running, invisible.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if process_start_time(prototype_pid).as_deref() != Some(start_time_before.as_str()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the wedged prototype pid={prototype_pid} is still alive - it was replaced but never killed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And the pool actually recovers rather than just failing politely.
    let recovery_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let resp = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/", server.port))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        if matches!(&resp, Ok(r) if r.status().is_success()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "master never served a request again after the wedged prototype was replaced: {resp:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Without percent-decoding, a request for `/spaced%20name.txt` looked for
/// a file literally named `spaced%20name.txt` and 404'd - every path with
/// an escaped character was unreachable.
#[tokio::test]
async fn a_percent_encoded_static_path_reaches_the_real_file() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "percent-decode-static",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/spaced%20name.txt",
        server.port
    ))
    .await
    .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "an encoded space must resolve to the real filename"
    );
    assert_eq!(resp.text().await.unwrap(), "hello from a spaced filename\n");
}

/// Sends a request target verbatim, bypassing the client-side URL
/// normalisation that would otherwise resolve and remove hostile segments
/// before the request is even sent. An attacker is under no such constraint.
async fn raw_get(port: u16, raw_target: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let req = format!("GET {raw_target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response to {raw_target:?}: {text:?}"));
    (status, text)
}

/// The traversal check is component-wise over the decoded path, so escapes
/// must be decoded before it runs - and an encoded separator refused outright
/// rather than decoded into one the checks never saw.
#[tokio::test]
async fn encoded_traversal_and_encoded_separators_are_rejected() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "percent-decode-traversal",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    for path in [
        "/%2e%2e/%2e%2e/etc/passwd", // decodes to ../../etc/passwd
        "/static/%2e%2e/%2e%2e/etc/passwd",
        "/%2e%2e%2fetc/passwd", // encoded separator
        "/a%2Fb",
        "/a%00b",  // NUL would truncate any C string built from it
        "/a%zz",   // malformed escape
        "/%FF%FE", // decodes to non-UTF-8
    ] {
        let (status, body) = raw_get(server.port, path).await;
        assert_eq!(status, 400, "{path} must be refused outright, got {status}");
        // Whatever else happens, no file content may come back.
        assert!(
            !body.contains("root:"),
            "{path} returned something that looks like /etc/passwd"
        );
    }
}

/// Each target must reach the server still encoded. Without this, the test
/// above could pass because the paths never arrived in hostile form at all.
#[tokio::test]
async fn a_raw_request_target_reaches_the_server_unnormalised() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "raw-target-sanity",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    // An ordinary encoded path arrives encoded and is decoded server-side
    // to a real file - proving the target crossed the wire verbatim.
    let (status, body) = raw_get(server.port, "/spaced%20name.txt").await;
    assert_eq!(status, 200);
    assert!(body.contains("hello from a spaced filename"), "got: {body}");
}

/// PATH_INFO reaches PHP decoded (RFC 3875 §4.1.5) while REQUEST_URI keeps
/// the raw form, which is what every other SAPI reports and what apps
/// re-parse for routing.
#[tokio::test]
async fn php_gets_a_decoded_path_info_and_a_raw_request_uri() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "percent-decode-php",
        www.to_str().unwrap(),
        serde_json::json!({}),
    )
    .await;

    let resp = reqwest::get(format!("http://127.0.0.1:{}/a%20b/c?q=%20x", server.port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("PATH_INFO=/a b/c"),
        "PATH_INFO must be decoded, got: {body}"
    );
    assert!(
        body.contains("URI=/a%20b/c?q=%20x"),
        "REQUEST_URI must stay raw, got: {body}"
    );
}

/// httpoxy (CVE-2016-5385): a client-set `Proxy:` header must never become
/// `$_SERVER['HTTP_PROXY']`, which PHP HTTP clients read as their outbound
/// proxy.
///
/// Two independent layers hold this, so it passes with either removed. A
/// property test for the deployed whole, not a guard on our own filter.
#[tokio::test]
async fn a_client_supplied_proxy_header_never_reaches_php() {
    let www = fixtures_dir().join("www");
    let server = start_server("httpoxy", www.to_str().unwrap(), serde_json::json!({})).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/proxy-header-check",
            server.port
        ))
        .header("Proxy", "http://attacker.example:8080")
        .header("X-Test", "sentinel")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("HTTP_PROXY=MISSING"),
        "the Proxy header must be stripped, got: {body}"
    );
    // The sentinel proves ordinary headers still get through - a filter
    // that dropped everything would pass the assertion above for free.
    assert!(
        body.contains("HEADER_X_TEST=sentinel"),
        "unrelated headers must still reach PHP, got: {body}"
    );
}

/// Shrinking the header buffer resets its bookkeeping mid-life, on a buffer
/// every later request reuses, so getting it wrong corrupts the *next*
/// response rather than the one that triggered it. The property is that one
/// worker survives the whole cycle.
///
/// A pool of one keeps every request on that worker, which is what makes this
/// exercise reuse at all.
#[tokio::test]
async fn a_worker_survives_repeated_oversized_header_responses() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "header-buffer-shrink",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 1, "spare": 1 }, "limits": { "requests": 1000 } } }),
    )
    .await;
    let client = reqwest::Client::new();
    let worker_before = worker_pids(&server).await;

    // Large enough to trip the shrink, so each leaves the buffer shrunk
    // behind it.
    let big = |port: u16| format!("http://127.0.0.1:{port}/many-headers?n=50&vsize=6000&csp=1");

    for round in 0..3 {
        let resp = client.get(big(server.port)).send().await.unwrap();
        assert_eq!(
            resp.status(),
            200,
            "round {round}: oversized header response failed"
        );
        let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
        assert_eq!(cookies.len(), 50, "round {round}: lost Set-Cookie headers");
        assert_eq!(
            cookies[49].to_str().unwrap(),
            format!("cookie_49={}; Path=/", "v".repeat(6000)),
            "round {round}: last cookie came back damaged"
        );
        assert_eq!(
            resp.headers()
                .get("content-security-policy")
                .unwrap()
                .to_str()
                .unwrap(),
            expected_csp(),
            "round {round}: CSP came back damaged"
        );

        // An ordinary response between the big ones: this is the one that
        // runs against the freshly shrunk buffer.
        let small = client
            .get(format!("http://127.0.0.1:{}/", server.port))
            .send()
            .await
            .unwrap();
        assert_eq!(
            small.status(),
            200,
            "round {round}: small request after a shrink failed"
        );
        assert!(
            small
                .text()
                .await
                .unwrap()
                .contains("PHP response, worker pid="),
            "round {round}: small response body was damaged"
        );
    }

    // If the worker had been recycled or replaced mid-test, the buffer
    // reuse this is meant to exercise never happened.
    assert_eq!(
        worker_pids(&server).await,
        worker_before,
        "the same worker must have served every request"
    );
}

/// Starts a server with stdout piped, returning the child plus a shared
/// buffer of every line it writes. `TestServer` always discards stdout, so
/// any test that reads the access log needs this instead.
async fn start_server_capturing_stdout(
    name: &str,
    port: u16,
    status_port: u16,
    config: serde_json::Value,
) -> (Child, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    let config_path = std::env::temp_dir().join(format!("test-config-{name}.json"));
    std::fs::File::create(&config_path)
        .unwrap()
        .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_proteus"))
        .arg(&config_path)
        .env("PROTEUS_PHP_MOD_PATH", php_mod_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server binary");
    let stdout = child.stdout.take().unwrap();
    let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let writer = std::sync::Arc::clone(&lines);
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            writer.lock().unwrap().push(line);
        }
    });

    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "server never became ready"
        );
        match client
            .get(format!("http://127.0.0.1:{status_port}/"))
            .timeout(Duration::from_millis(500))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => break,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let _ = port;
    (child, lines)
}

/// Waits for an access-log line matching `pick`.
async fn await_access_log(
    lines: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    pick: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let found = lines.lock().unwrap().iter().find_map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).ok()?;
            (v.get("type")?.as_str()? == "access_log" && pick(&v)).then_some(v)
        });
        if let Some(entry) = found {
            return entry;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no matching access log line appeared"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The log must be written when the body finishes, not when the headers were
/// ready: otherwise `duration_ms` excludes the whole transfer and a stream
/// that dies halfway is recorded as a clean 200.
///
/// The 200 is already on the wire and cannot be taken back, so the log line is
/// the only place that failure can ever surface.
#[tokio::test]
async fn the_access_log_records_a_body_that_failed_after_the_headers_went_out() {
    let www = fixtures_dir().join("www");
    let (port, status_port) = (next_port(), next_port());
    let (mut child, lines) = start_server_capturing_stdout(
        "accesslog-body-failed",
        port,
        status_port,
        serde_json::json!({
            "listen": [format!("127.0.0.1:{port}")],
            "status": { "listen": format!("127.0.0.1:{status_port}") },
            "routes": [ { "match": {}, "action": "php", "target": "default" } ],
            "php": {
                "targets": { "default": { "root": www.to_str().unwrap(), "script": "index.php" } },
                // Explicit, not this environment's default (4096): the
                // fixture's flush() has to reach ub_write immediately or
                // there is no in-flight body to interrupt.
                "options": { "admin": { "output_buffering": "0" } },
                "limits": { "requests": 100, "timeout": 60 },
                // max 1 makes the pid in workers[] unambiguously the one
                // serving this request.
                "processes": { "max": 1, "spare": 1 },
                "queue": { "timeout": 10 }
            }
        }),
    )
    .await;
    let _guard = ChildGuard(&mut child);
    let client = reqwest::Client::new();

    let mut resp = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .get(format!("http://127.0.0.1:{port}/stream-then-die"))
            .send(),
    )
    .await
    .expect("headers never arrived")
    .unwrap();
    assert_eq!(resp.status(), 200);
    let first = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
        .await
        .expect("first chunk never arrived")
        .expect("reading the first chunk failed")
        .expect("stream ended before any chunk");
    assert_eq!(&first[..], b"first-chunk\n");

    let status: serde_json::Value = client
        .get(format!("http://127.0.0.1:{status_port}/"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pid = status["php"]["workers"][0]["pid"]
        .as_i64()
        .expect("exactly one worker");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();

    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        while let Ok(Some(_)) = resp.chunk().await {}
    })
    .await;
    assert!(
        drained.is_ok(),
        "response never ended after its worker was killed"
    );

    let entry = await_access_log(&lines, |v| v["path"].as_str() == Some("/stream-then-die")).await;
    assert_eq!(
        entry["status"].as_u64(),
        Some(200),
        "the status was already committed: {entry}"
    );
    assert_ne!(
        entry["body"].as_str(),
        Some("complete"),
        "a body that died mid-stream must not be logged as complete: {entry}"
    );
}

/// The other half: `duration_ms` has to cover the body, not just the
/// headers. `/slow-stream` takes ~1s to emit its five chunks while its
/// headers go out immediately.
#[tokio::test]
async fn access_log_duration_covers_the_body_transfer_not_just_the_headers() {
    let www = fixtures_dir().join("www");
    let (port, status_port) = (next_port(), next_port());
    let (mut child, lines) = start_server_capturing_stdout(
        "accesslog-duration",
        port,
        status_port,
        serde_json::json!({
            "listen": [format!("127.0.0.1:{port}")],
            "status": { "listen": format!("127.0.0.1:{status_port}") },
            "routes": [ { "match": {}, "action": "php", "target": "default" } ],
            "php": {
                "targets": { "default": { "root": www.to_str().unwrap(), "script": "index.php" } },
                "options": { "admin": { "output_buffering": "0" } },
                "limits": { "requests": 100, "timeout": 60 },
                "processes": { "max": 2, "spare": 1 },
                "queue": { "timeout": 10 }
            }
        }),
    )
    .await;
    let _guard = ChildGuard(&mut child);

    let resp = reqwest::get(format!("http://127.0.0.1:{port}/slow-stream"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("chunk-4"),
        "the whole body should have arrived: {body}"
    );

    let entry = await_access_log(&lines, |v| v["path"].as_str() == Some("/slow-stream")).await;
    assert_eq!(entry["body"].as_str(), Some("complete"), "got: {entry}");
    // Five chunks, 200ms apart - a duration measured at header time would
    // be a handful of milliseconds.
    assert!(
        entry["duration_ms"].as_u64().unwrap() >= 800,
        "duration_ms must include the body transfer, got: {entry}"
    );
}

/// Slowloris: a client dribbling a request head it never finishes. Unbounded,
/// such a connection holds a task and a socket indefinitely having sent
/// nothing actionable, and enough of them starve everyone else.
#[tokio::test]
async fn an_unfinished_request_head_is_cut_off_by_the_header_read_timeout() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "slowloris",
        www.to_str().unwrap(),
        serde_json::json!({ "connection": { "header_read_timeout": 2, "idle_timeout": 0 } }),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    // A request head with no terminating blank line: complete-looking, and
    // never finished.
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    let started = tokio::time::Instant::now();
    let mut sink = Vec::new();
    // Returns once the server hangs up. Without the timeout this blocks
    // until the test's own deadline.
    let closed = tokio::time::timeout(Duration::from_secs(15), stream.read_to_end(&mut sink)).await;
    assert!(
        closed.is_ok(),
        "the server never closed a connection that never finished its request head"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "connection lingered {:?}, far past the 2s header_read_timeout",
        started.elapsed()
    );
}

/// The slowloris above, moved past the head. Both other timeouts are set
/// short here, so a connection outliving them proves neither one reaches a
/// body that stopped arriving.
#[tokio::test]
async fn a_request_body_that_never_arrives_does_not_hold_a_connection_forever() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "stalled-body",
        www.to_str().unwrap(),
        serde_json::json!({
            "connection": { "header_read_timeout": 2, "idle_timeout": 2, "body_read_timeout": 2 }
        }),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    // A complete head - so the head timeout is satisfied - promising a body
    // that then never finishes arriving.
    stream
        .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048576\r\n\r\n")
        .await
        .unwrap();
    stream.write_all(b"x").await.unwrap();
    stream.flush().await.unwrap();

    let started = tokio::time::Instant::now();
    let mut sink = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(15), stream.read_to_end(&mut sink)).await;
    assert!(
        closed.is_ok(),
        "the server never closed a connection whose body stopped arriving: \
         header_read_timeout does not cover the body, and the request stays in \
         flight so idle_timeout cannot either"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "connection lingered {:?} past both configured timeouts",
        started.elapsed()
    );
    assert!(
        String::from_utf8_lossy(&sink).starts_with("HTTP/1.1 408"),
        "a stalled body should be answered, not just dropped, got: {:?}",
        String::from_utf8_lossy(&sink)
            .chars()
            .take(80)
            .collect::<String>()
    );
}

/// A body taking several times `body_read_timeout` in total while never
/// pausing that long must survive: cutting off a slow but honest client would
/// make the fix worse than the hole it closes.
#[tokio::test]
async fn a_slow_but_steady_request_body_is_not_cut_off() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "slow-steady-body",
        www.to_str().unwrap(),
        serde_json::json!({ "connection": { "body_read_timeout": 2, "idle_timeout": 0 } }),
    )
    .await;

    let chunks = ["alpha-", "bravo-", "charlie-", "delta-", "echo-", "foxtrot"];
    let expected: String = chunks.concat();

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /echo-body HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
                expected.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    for chunk in chunks {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        stream.write_all(chunk.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

    let mut sink = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut sink)).await;
    assert!(
        read.is_ok(),
        "the server never answered a slow but steady upload"
    );
    let response = String::from_utf8_lossy(&sink);
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "slow steady upload was rejected: {response}"
    );
    assert!(
        response.contains(&expected),
        "body did not survive a slow upload: {response}"
    );
}

/// The connection cap only helps if idle connections go away: otherwise an
/// attacker fills every slot with keep-alives that went quiet after one cheap
/// request, and the cap becomes the denial of service.
#[tokio::test]
async fn an_idle_keep_alive_connection_is_eventually_closed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let www = fixtures_dir().join("www");
    let server = start_server(
        "idle-keepalive",
        www.to_str().unwrap(),
        serde_json::json!({ "connection": { "idle_timeout": 2, "header_read_timeout": 30 } }),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    // Read the response but keep the connection open and silent after it.
    let mut buf = [0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(n > 0, "no response arrived");
    assert!(
        String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"),
        "unexpected response"
    );

    let started = tokio::time::Instant::now();
    let mut sink = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut sink)).await;
    assert!(
        closed.is_ok(),
        "an idle keep-alive connection was never closed"
    );
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "idle connection lingered {:?}, far past the 2s idle_timeout",
        started.elapsed()
    );
}

/// A request slower than `idle_timeout` must still complete.
///
/// What holds this up is the graceful shutdown, which finishes an in-flight
/// request regardless of the connection's own state tracking; that narrower
/// job is pinned separately.
#[tokio::test]
async fn a_slow_request_is_not_mistaken_for_an_idle_connection() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "idle-vs-slow-request",
        www.to_str().unwrap(),
        // The request takes 3x the idle timeout, entirely inside PHP.
        serde_json::json!({
            "connection": { "idle_timeout": 2, "header_read_timeout": 30 },
            "php": { "limits": { "requests": 100, "timeout": 60 } }
        }),
    )
    .await;

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        reqwest::get(format!("http://127.0.0.1:{}/?delay_ms=6000", server.port)),
    )
    .await
    .expect("request never completed - the idle watcher closed a busy connection")
    .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.text()
            .await
            .unwrap()
            .contains("PHP response, worker pid="),
        "the body must arrive intact"
    );
}

/// Same property for a streamed body, where `handle` has already returned
/// while bytes are still being written. Also upheld by the graceful
/// shutdown rather than by `ConnState` alone.
#[tokio::test]
async fn a_slowly_streaming_response_is_not_mistaken_for_an_idle_connection() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "idle-vs-slow-stream",
        www.to_str().unwrap(),
        serde_json::json!({
            "connection": { "idle_timeout": 1, "header_read_timeout": 30 },
            "php": { "options": { "admin": { "output_buffering": "0" } }, "limits": { "requests": 100, "timeout": 60 } }
        }),
    )
    .await;

    // /slow-stream emits 5 chunks 200ms apart - each gap is longer than
    // nothing, and the whole body outlasts the 1s idle timeout.
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        reqwest::get(format!("http://127.0.0.1:{}/slow-stream", server.port)),
    )
    .await
    .expect("headers never arrived")
    .unwrap();
    assert_eq!(resp.status(), 200);
    let body = tokio::time::timeout(Duration::from_secs(30), resp.text())
        .await
        .expect("body never finished - the idle watcher cut a streaming response")
        .unwrap();
    assert!(
        body.contains("chunk-0") && body.contains("chunk-4"),
        "streamed body was truncated: {body}"
    );
}

/// What the in-flight tracking is actually for.
///
/// The request completes either way. The difference is the connection
/// afterwards: treating a quiet socket as idle closes it behind that request,
/// so a keep-alive client reconnects for every slow request it makes.
///
/// Raw socket, because a client pool would transparently open a new connection
/// and hide exactly this regression.
#[tokio::test]
async fn keep_alive_survives_a_request_slower_than_the_idle_timeout() {
    use tokio::io::AsyncWriteExt;
    let www = fixtures_dir().join("www");
    let server = start_server(
        "keepalive-after-slow-request",
        www.to_str().unwrap(),
        serde_json::json!({
            "connection": { "idle_timeout": 2, "header_read_timeout": 30 },
            "php": { "limits": { "requests": 100, "timeout": 60 } }
        }),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();

    // Request one takes 2x the idle timeout, all of it inside PHP.
    stream
        .write_all(b"GET /?delay_ms=4000 HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let first = read_one_chunked_response(&mut stream, Duration::from_secs(30))
        .await
        .expect("slow request never answered");
    assert!(
        first.starts_with("HTTP/1.1 200"),
        "slow request failed: {first}"
    );

    // Request two, immediately, on the same connection.
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let second = read_one_chunked_response(&mut stream, Duration::from_secs(15))
        .await
        .expect("the connection was closed behind the slow request - keep-alive was lost");
    assert!(
        second.starts_with("HTTP/1.1 200"),
        "second request on the reused connection failed: {second}"
    );
}

/// Reads exactly one chunked response, stopping at the terminating zero
/// chunk. A single `read` is not one response: on a reused connection it
/// would hand back the tail of the previous body. `None` if the peer closed
/// first.
async fn read_one_chunked_response(
    stream: &mut tokio::net::TcpStream,
    limit: Duration,
) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let mut acc = Vec::new();
    let mut buf = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let n = tokio::time::timeout(remaining, stream.read(&mut buf))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            return None; // peer hung up mid-response
        }
        acc.extend_from_slice(&buf[..n]);
        if acc.windows(7).any(|w| w == b"\r\n0\r\n\r\n") {
            return Some(String::from_utf8_lossy(&acc).into_owned());
        }
    }
}

/// An unservable request is a property of the request, not of the worker that
/// would have taken it. The head cap refuses it before any worker is involved,
/// so the pool must be untouched and the counters quiet.
#[tokio::test]
async fn an_oversized_request_is_refused_without_costing_the_pool_a_worker() {
    let www = fixtures_dir().join("www");
    let server = start_server("oversized-req", www.to_str().unwrap(), serde_json::json!({})).await;

    let pids_before = worker_pids(&server).await;
    assert!(!pids_before.is_empty(), "expected pre-spawned spare workers");

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/echo-body", server.port))
        .header("X-Big", "h".repeat(180 * 1024))
        .body("b".repeat(31 * 1024))
        .send()
        .await
        .expect("the connection must survive an unservable request");
    assert_eq!(
        response.status(),
        431,
        "an unservable request must be refused as a client error, not a 500"
    );

    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        status["php"]["counters"]["prototype_respawns_total"], 0,
        "refusing a request must not read as a sick prototype"
    );
    assert_eq!(
        status["php"]["counters"]["requests_failed"], 0,
        "a refused request is not a dispatch failure"
    );

    assert_eq!(
        worker_pids(&server).await,
        pids_before,
        "the pool must be exactly as it was"
    );

    // And the pool still serves.
    let ok = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/echo-body", server.port))
        .body("b".repeat(1024))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    assert_eq!(ok.bytes().await.unwrap().len(), 1024);
}

/// Either side of the inline/spill boundary must reach PHP byte-exact: the
/// ring carries small bodies in the request frame and large ones by filename.
#[tokio::test]
async fn bodies_on_both_sides_of_the_spill_threshold_reach_php_intact() {
    let www = fixtures_dir().join("www");
    let server = start_server("spill-boundary", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::new();

    // Straddling `BODY_MEMORY_THRESHOLD`, which this crate cannot see: keep
    // these either side of it if it moves.
    for size in [1024, 62 * 1024, 63 * 1024, 64 * 1024, 256 * 1024] {
        let response = client
            .post(format!("http://127.0.0.1:{}/echo-body", server.port))
            .body("b".repeat(size))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{size} byte body failed: {e}"));
        assert_eq!(response.status(), 200, "{size} byte body");
        assert_eq!(
            response.bytes().await.unwrap().len(),
            size,
            "{size} byte body did not reach PHP whole"
        );
    }
}

/// A worker's eventfds join the reactor of whichever runtime dispatches to it
/// and leave when it is returned, so a worker moves between runtimes over its
/// life. Fresh connections spread across cores, which is what moves it.
#[tokio::test]
async fn workers_survive_being_dispatched_from_one_runtime_after_another() {
    let www = fixtures_dir().join("www");
    let server = start_server(
        "runtime-hopping",
        www.to_str().unwrap(),
        serde_json::json!({ "php": { "processes": { "max": 4, "spare": 2 } } }),
    )
    .await;

    for round in 0..8 {
        let mut inflight = Vec::new();
        for i in 0..16 {
            // A client of its own each time, so every request opens a fresh
            // connection rather than reusing one runtime's keep-alive.
            let url = format!("http://127.0.0.1:{}/echo-body", server.port);
            inflight.push(tokio::spawn(async move {
                reqwest::Client::new()
                    .post(url)
                    .body(format!("round{round}-req{i}"))
                    .send()
                    .await?
                    .text()
                    .await
            }));
        }
        for (i, task) in inflight.into_iter().enumerate() {
            let body = task
                .await
                .expect("request task panicked")
                .unwrap_or_else(|e| panic!("round {round} request {i} failed: {e}"));
            assert_eq!(
                body,
                format!("round{round}-req{i}"),
                "round {round} request {i} came back wrong"
            );
        }
    }

    let status: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/", server.status_port))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        status["php"]["counters"]["prototype_respawns_total"], 0,
        "moving workers between runtimes must not look like a sick prototype"
    );
    assert_eq!(
        status["php"]["counters"]["workers_reaped_dead"], 0,
        "no worker should have died from being dispatched on a different runtime"
    );
    assert_eq!(
        status["php"]["processes"]["max"], 4,
        "the pool must still be at full strength"
    );
}

/// Every core accepts on every configured address, so a second address is a
/// second accept loop per core rather than a second set of threads. A core that
/// only ever ran the first one would leave the second served by nobody.
#[tokio::test]
async fn every_configured_address_is_served() {
    let www = fixtures_dir().join("www");
    let second = next_port();
    let server = start_server(
        "multi-listen",
        www.to_str().unwrap(),
        serde_json::json!({ "listen_extra": second }),
    )
    .await;

    // Enough requests that the kernel spreads them over more than one core's
    // socket; a per-core mistake would show as a hang on some of them.
    for _ in 0..20 {
        for port in [server.port, second] {
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                reqwest::get(format!("http://127.0.0.1:{port}/app")),
            )
            .await
            .unwrap_or_else(|_| panic!("port {port} never answered"))
            .unwrap_or_else(|e| panic!("port {port} failed: {e}"));
            assert_eq!(response.status(), 200, "port {port}");
        }
    }
}

/// The body fd goes only after its request frame is on the ring, so a frame
/// that failed to go leaves nothing queued - otherwise the next spilled body
/// picks up the wrong fd and serves another client's bytes.
#[tokio::test]
async fn a_refused_request_leaves_no_body_fd_for_the_next_one_to_pick_up() {
    let www = fixtures_dir().join("www");
    let server = start_server("body-fd-order", www.to_str().unwrap(), serde_json::json!({})).await;
    let client = reqwest::Client::new();

    // Spilled body plus headers too big for a ring frame: the frame is
    // refused after the body file already exists.
    let refused = client
        .post(format!("http://127.0.0.1:{}/echo-body", server.port))
        // One entry, so it cannot be split across frames and is genuinely
        // unservable rather than merely large.
        .header("X-Big", "h".repeat(180 * 1024))
        .body("a".repeat(64 * 1024))
        .send()
        .await
        .expect("the connection must survive a refused request");
    assert_eq!(
        refused.status(),
        431,
        "an unservable request must be refused as a client error"
    );

    // The next spilled body must be this request's own, byte for byte.
    let mine = "b".repeat(64 * 1024);
    let response = client
        .post(format!("http://127.0.0.1:{}/echo-body", server.port))
        .body(mine.clone())
        .send()
        .await
        .expect("the pool must still serve");
    assert_eq!(response.status(), 200);
    let got = response.text().await.unwrap();
    assert_eq!(
        got.len(),
        mine.len(),
        "the body that came back is the wrong length"
    );
    assert_eq!(got, mine, "a stale body fd was picked up by the next request");
}

/// The head cap is what keeps a request inside one ring frame, so it has to
/// hold: a header set past it is refused before any of this reaches a worker,
/// and one just under it is served whole.
#[tokio::test]
async fn the_request_head_cap_holds_on_both_sides() {
    let www = fixtures_dir().join("www");
    let server = start_server("head-cap", www.to_str().unwrap(), serde_json::json!({})).await;
    let pids_before = worker_pids(&server).await;

    // Comfortably inside the cap, and each value carries its own index so PHP
    // catches a header lost or reordered on the way.
    const PROBES: usize = 16;
    let values: Vec<String> = (0..PROBES)
        .map(|i| format!("{i}-{}", "v".repeat(4 * 1024)))
        .collect();
    let mut request =
        reqwest::Client::new().get(format!("http://127.0.0.1:{}/probe-headers", server.port));
    for (i, v) in values.iter().enumerate() {
        request = request.header(format!("X-Probe-{i}"), v);
    }
    let response = request.send().await.expect("a head under the cap must serve");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.text().await.unwrap(),
        format!("ok:{PROBES}"),
        "the header set did not reach PHP whole"
    );

    // Past the cap: refused before a worker is ever involved.
    let mut oversized =
        reqwest::Client::new().get(format!("http://127.0.0.1:{}/probe-headers", server.port));
    for i in 0..40 {
        oversized = oversized.header(format!("X-Big-{i}"), "v".repeat(4 * 1024));
    }
    let refused = oversized
        .send()
        .await
        .expect("the connection must survive a refused head");
    assert_eq!(refused.status(), 431);
    assert_eq!(
        worker_pids(&server).await,
        pids_before,
        "a head refused at the door must cost the pool nothing"
    );
}

/// One header far past the cap: refused like any other oversized head, and
/// still without costing the pool a worker.
#[tokio::test]
async fn one_indivisible_header_value_is_still_refused() {
    let www = fixtures_dir().join("www");
    let server =
        start_server("header-indivisible", www.to_str().unwrap(), serde_json::json!({})).await;
    let pids_before = worker_pids(&server).await;

    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/app", server.port))
        .header("X-Big", "h".repeat(200 * 1024))
        .send()
        .await
        .expect("the connection must survive");
    assert_eq!(response.status(), 431);
    assert_eq!(
        worker_pids(&server).await,
        pids_before,
        "refusing must still cost no worker"
    );
}

/// `QUERY_STRING` is no longer carried on the wire: the worker takes it from
/// `REQUEST_URI` past the first `?`. PHP must not be able to tell.
#[tokio::test]
async fn query_string_matches_the_request_uri_it_is_derived_from() {
    let www = fixtures_dir().join("www");
    let server = start_server("cgi-vars", www.to_str().unwrap(), serde_json::json!({})).await;

    for (target, want_query, want_a) in [
        ("/cgi-vars?a=1&b=two", "a=1&b=two", "1"),
        ("/cgi-vars", "", "MISSING"),
        // A bare `?` and an encoded one in a value: the split is on the first
        // `?` only, and what follows is passed through untouched.
        ("/cgi-vars?", "", "MISSING"),
        ("/cgi-vars?a=x%3Fy&c=3", "a=x%3Fy&c=3", "x?y"),
        // A second `?` is ordinary query data: the split is on the first one
        // only, so everything after it goes through as-is.
        ("/cgi-vars?a=1?b=2", "a=1?b=2", "1?b=2"),
    ] {
        let body = reqwest::get(format!("http://127.0.0.1:{}{target}", server.port))
            .await
            .unwrap_or_else(|e| panic!("{target}: {e}"))
            .text()
            .await
            .unwrap();
        assert!(
            body.contains(&format!("uri={target}\n")),
            "{target}: REQUEST_URI wrong, got {body:?}"
        );
        assert!(
            body.contains(&format!("query={want_query}\n")),
            "{target}: QUERY_STRING wrong, got {body:?}"
        );
        assert!(
            body.contains(&format!("get_a={want_a}\n")),
            "{target}: $_GET did not parse, got {body:?}"
        );
    }
}
