use super::*;

fn header_to_cgi_var(name: &str) -> String {
    let mut out = Vec::new();
    write_header_cgi_key(&mut out, name);
    String::from_utf8(out).unwrap()
}

#[test]
fn header_names_become_cgi_vars() {
    assert_eq!(header_to_cgi_var("User-Agent"), "HTTP_USER_AGENT");
    assert_eq!(header_to_cgi_var("x-request-id"), "HTTP_X_REQUEST_ID");
    assert_eq!(header_to_cgi_var("Host"), "HTTP_HOST");
}

#[test]
fn cgi_var_buf_rolls_back_an_entry_with_an_embedded_nul() {
    let mut vars = CgiVarBuf::with_capacity(64);
    vars.push("BEFORE", format_args!("ok"));
    vars.push("BAD", format_args!("has\0nul"));
    vars.push("AFTER", format_args!("also-ok"));
    let ptrs = vars.pointers();
    assert_eq!(
        ptrs.len(),
        2,
        "the NUL-containing entry must be dropped, not just truncated"
    );
    let as_str = |p: *const c_char| unsafe { std::ffi::CStr::from_ptr(p) }.to_str().unwrap();
    assert_eq!(as_str(ptrs[0]), "BEFORE=ok");
    assert_eq!(as_str(ptrs[1]), "AFTER=also-ok");
}

#[test]
fn parses_captured_header_lines() {
    assert_eq!(parse_headers(b"").iter().count(), 0);
    assert_eq!(
        parse_headers(b"X-Test: hello\nSet-Cookie: a=1")
            .iter()
            .collect::<Vec<_>>(),
        vec![("X-Test", "hello"), ("Set-Cookie", "a=1")]
    );
    // A value containing its own colon must not be truncated.
    assert_eq!(
        parse_headers(b"Location: https://example.com/x")
            .iter()
            .collect::<Vec<_>>(),
        vec![("Location", "https://example.com/x")]
    );
}

/// Checked directly rather than through PHP, because an end-to-end test
/// cannot pin the `Proxy` entry: PHP refuses to register `HTTP_PROXY` itself,
/// so removing this filter leaves the observable behaviour identical.
#[test]
fn suppressed_request_headers_cover_underscore_framing_and_httpoxy() {
    for name in [
        "X_Foo_Bar",
        "x_foo",
        "Content-Type",
        "content-length",
        "CONTENT-TYPE",
        "Proxy",
        "PROXY",
        "pRoXy",
    ] {
        assert!(
            suppressed_request_header(name),
            "{name} must never reach $_SERVER"
        );
    }
    for name in [
        "X-Foo-Bar",
        "User-Agent",
        "Cookie",
        "Authorization",
        "Proxy-Authorization",
        "X-Proxy",
    ] {
        assert!(
            !suppressed_request_header(name),
            "{name} is a legitimate header and must pass through"
        );
    }
}

#[test]
fn cstrings_or_err_rejects_an_embedded_nul() {
    let ok = cstrings_or_err(&["foo=bar".to_string()]).expect("no NUL, should succeed");
    assert_eq!(ok[0].to_str().unwrap(), "foo=bar");

    let err = cstrings_or_err(&["foo=b\0ar".to_string()]);
    assert!(
        err.is_err(),
        "an embedded NUL must be a real error, not silently become an empty, no-op CString"
    );
}

/// `kind` is fully controlled by our own C code; anything outside the three
/// known constants can only mean an ABI mismatch, and must not be silently
/// forwarded as `End` (or as anything else).
#[test]
fn chunk_trampoline_drops_an_unrecognized_kind_rather_than_treating_it_as_end() {
    let mut seen: Vec<&'static str> = Vec::new();
    let mut closure = |chunk: PhpChunk| {
        seen.push(match chunk {
            PhpChunk::Headers { .. } => "headers",
            PhpChunk::Body(_) => "body",
            PhpChunk::End => "end",
        });
    };
    let client_gone = std::sync::atomic::AtomicBool::new(false);
    let mut ctx = ChunkCtx {
        on_chunk: &mut closure,
        client_gone: &client_gone,
    };
    let user_data = &mut ctx as *mut _ as *mut c_void;

    unsafe {
        chunk_trampoline(99, 0, std::ptr::null(), 0, user_data);
    }
    drop(ctx);

    assert!(
        seen.is_empty(),
        "an unrecognized kind must not be forwarded as any chunk, least of all End: got {seen:?}"
    );
}
