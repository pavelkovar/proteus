use super::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A source that only ever answers from the blocking pool, without needing
/// a real file.
struct FakeBlockingSource {
    remaining: usize,
    pending: Option<tokio::task::JoinHandle<Bytes>>,
}

impl Stream for FakeBlockingSource {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pending.is_none() {
            if self.remaining == 0 {
                return Poll::Ready(None);
            }
            self.remaining -= 1;
            self.pending = Some(tokio::task::spawn_blocking(|| Bytes::from_static(b"chunk")));
        }
        match Pin::new(self.pending.as_mut().unwrap()).poll(cx) {
            Poll::Ready(res) => {
                self.pending = None;
                Poll::Ready(Some(Ok(res.expect("spawn_blocking panicked"))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn drain(body: ResponseBody) -> Bytes {
    body.collect()
        .await
        .expect("body stream errored")
        .to_bytes()
}

/// A single blocking thread reproduces the nested-`spawn_blocking` deadlock
/// `compressed_body`'s doc warns about.
#[test]
fn compressed_body_does_not_deadlock_when_source_itself_needs_a_blocking_thread() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();

    let result = rt.block_on(async {
        let source = FakeBlockingSource {
            remaining: 20,
            pending: None,
        };
        let body = compressed_body(source, Encoding::Gzip, None);
        tokio::time::timeout(std::time::Duration::from_secs(5), drain(body)).await
    });

    // A reintroduced deadlock leaves a stuck blocking task, and dropping a
    // `Runtime` waits for exactly that - a hung binary instead of a failure.
    rt.shutdown_timeout(std::time::Duration::from_secs(1));

    let collected = result.expect("compressed_body hung - blocking-pool deadlock regression");
    assert!(!collected.is_empty());
}

// --- encoder window sizing ---

#[test]
fn window_log_covers_the_body_and_stays_in_zstds_accepted_range() {
    assert_eq!(window_log_for(Some(657)), 10, "657 fits in 2^10");
    assert_eq!(window_log_for(Some(1 << 12)), 13);
    assert_eq!(window_log_for(Some((1 << 12) + 1)), 13);
    assert_eq!(window_log_for(Some(200_000)), 18);
    assert_eq!(
        window_log_for(Some(64 << 20)),
        WINDOW_LOG_MAX,
        "clamped, not grown"
    );
    assert_eq!(
        window_log_for(Some(0)),
        WINDOW_LOG_MIN,
        "zstd rejects anything smaller"
    );
    assert_eq!(window_log_for(None), WINDOW_LOG_MAX);
}

fn zstd_roundtrip(body_len: usize, size_hint: Option<u64>) -> Vec<u8> {
    let payload: Vec<u8> = (0..body_len)
        .map(|i| b"the quick brown fox "[i % 20])
        .collect();
    let chunks: Vec<std::io::Result<Bytes>> = payload
        .chunks(4096)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let compressed = rt.block_on(async {
        drain(compressed_body(
            tokio_stream::iter(chunks),
            Encoding::Zstd,
            size_hint,
        ))
        .await
    });
    let decoded =
        zstd::stream::decode_all(&compressed[..]).expect("emitted frame is not valid zstd");
    assert_eq!(decoded, payload, "compressed body does not round-trip");
    compressed.to_vec()
}

/// Why `size_hint` sets the window and never `set_pledged_src_size`: a
/// script's `Content-Length` is its own unverified claim, and pledging it
/// aborts the encode mid-body on the first byte past it.
#[test]
fn a_body_far_larger_than_its_declared_length_still_encodes_correctly() {
    zstd_roundtrip(256 * 1024, Some(8));
}

#[test]
fn a_body_far_smaller_than_its_declared_length_still_encodes_correctly() {
    zstd_roundtrip(64, Some(4 * 1024 * 1024));
}

/// A window sized to the body must not cost ratio on a body that fits in
/// it - the premise the sizing rests on.
#[test]
fn sizing_the_window_to_a_small_body_does_not_cost_compression() {
    let len = 4000;
    let sized = zstd_roundtrip(len, Some(len as u64)).len();
    let widest = zstd_roundtrip(len, Some(u64::from(u32::MAX))).len();
    assert!(
        sized <= widest,
        "sized window produced {sized} bytes vs {widest} at the max window"
    );
}

// --- streamed-response size gate ---

/// No declared length means the minimum-size gate is off entirely: a
/// streamed response cannot be measured before its headers go out, and
/// buffering to find out would cost time-to-first-byte.
#[test]
fn stream_size_gate_disables_the_minimum_when_no_length_was_declared() {
    let (body_len, min) = stream_size_gate(None, 1024);
    assert_eq!((body_len, min), (usize::MAX, 0));
    assert!(
        compression_eligible(body_len, min, "text/html", &[]),
        "an unmeasurable response must stay eligible regardless of min_size_bytes"
    );
}

/// A declared Content-Length is enough to apply the gate without buffering,
/// so it costs no time-to-first-byte.
#[test]
fn stream_size_gate_applies_the_minimum_when_the_script_declared_a_length() {
    let mime = vec!["text/html".to_string()];

    let (body_len, min) = stream_size_gate(Some(200), 1024);
    assert!(
        !compression_eligible(body_len, min, "text/html", &mime),
        "200 bytes is below the 1024 minimum"
    );
    assert_eq!(
        pick_encoding(body_len, "gzip", min, "text/html", &mime),
        None
    );

    let (body_len, min) = stream_size_gate(Some(4096), 1024);
    assert!(compression_eligible(body_len, min, "text/html", &mime));
    assert_eq!(
        pick_encoding(body_len, "gzip", min, "text/html", &mime),
        Some(Encoding::Gzip)
    );
}

/// The declared length is a hint, never trusted for framing, so a wrong
/// one may only mis-pick compression. Both directions must stay benign.
#[test]
fn a_wrong_declared_length_can_only_mis_decide_compression() {
    let mime = vec!["text/html".to_string()];
    // Understated: a big response goes out uncompressed. Wasteful, not wrong.
    let (body_len, min) = stream_size_gate(Some(1), 1024);
    assert_eq!(
        pick_encoding(body_len, "gzip", min, "text/html", &mime),
        None
    );
    // Overstated: a tiny response gets compressed - exactly the no-hint
    // behaviour, so no regression against it.
    let (body_len, min) = stream_size_gate(Some(999_999), 1024);
    assert_eq!(
        pick_encoding(body_len, "gzip", min, "text/html", &mime),
        Some(Encoding::Gzip)
    );
}

// --- Accept-Encoding negotiation edge cases ---
//
// These pin the behaviour the parser rewrite had to preserve: an explicit
// `q=0` and a wildcard interact in both directions (RFC 9110 §12.5.3), and
// nothing listed at all means "not acceptable", not "assume q=1".

/// Convenience: eligibility is orthogonal to negotiation, so these all
/// pass `true` and vary only the header.
fn pick(accept_encoding: &str) -> Option<Encoding> {
    pick_encoding_when_eligible(true, accept_encoding)
}

#[test]
fn an_explicit_q0_vetoes_a_coding_the_server_would_otherwise_prefer() {
    // zstd is first preference; vetoed, the next acceptable one wins.
    assert_eq!(pick("zstd;q=0, br, gzip"), Some(Encoding::Brotli));
    assert_eq!(pick("zstd;q=0, br;q=0, gzip"), Some(Encoding::Gzip));
    assert_eq!(pick("zstd;q=0, br;q=0, gzip;q=0"), None);
}

#[test]
fn a_wildcard_accepts_codings_the_client_never_named() {
    assert_eq!(pick("*"), Some(Encoding::Zstd));
    assert_eq!(pick("identity, *"), Some(Encoding::Zstd));
}

/// Both directions of the override: a specific entry beats `*` whichever
/// way round they appear, and whichever one is the permissive half.
#[test]
fn an_explicit_entry_overrides_the_wildcard_in_both_directions() {
    // Wildcard permits everything, but zstd is specifically vetoed.
    assert_eq!(pick("*, zstd;q=0"), Some(Encoding::Brotli));
    assert_eq!(pick("zstd;q=0, *"), Some(Encoding::Brotli));
    // Wildcard vetoes everything, but gzip is specifically allowed.
    assert_eq!(pick("*;q=0, gzip"), Some(Encoding::Gzip));
    assert_eq!(pick("gzip, *;q=0"), Some(Encoding::Gzip));
}

#[test]
fn nothing_listed_means_not_acceptable_rather_than_assumed() {
    assert_eq!(pick(""), None);
    assert_eq!(pick("identity"), None);
    assert_eq!(pick("deflate, compress"), None);
}

#[test]
fn negotiation_tolerates_whitespace_case_and_junk_q_values() {
    assert_eq!(pick("  GZIP  "), Some(Encoding::Gzip));
    assert_eq!(pick("Br ; q=0.5"), Some(Encoding::Brotli));
    // An unparseable q defaults to 1.0, same as an absent one.
    assert_eq!(pick("gzip;q=bogus"), Some(Encoding::Gzip));
    assert_eq!(pick("gzip;charset=utf-8"), Some(Encoding::Gzip));
    // Empty entries between separators must not break the walk.
    assert_eq!(pick(",, ,gzip,,"), Some(Encoding::Gzip));
}

/// A repeated coding keeps its first mention, matching the `find`-based
/// lookup this replaced.
#[test]
fn a_repeated_coding_keeps_its_first_mention() {
    assert_eq!(pick("gzip;q=0, gzip"), None);
    assert_eq!(pick("gzip, gzip;q=0"), Some(Encoding::Gzip));
}

/// Ineligible short-circuits before the header is even looked at.
#[test]
fn an_ineligible_response_never_negotiates() {
    assert_eq!(pick_encoding_when_eligible(false, "zstd, br, gzip"), None);
}

#[test]
fn compression_respects_size_threshold() {
    assert_eq!(
        pick_encoding(100, "gzip", 1024, "text/plain", &[]),
        None,
        "below threshold"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/plain", &[]),
        Some(Encoding::Gzip),
        "above threshold, gzip accepted"
    );
    assert_eq!(
        pick_encoding(2000, "", 1024, "text/plain", &[]),
        None,
        "above threshold but no accept-encoding"
    );
    assert_eq!(
        pick_encoding(2000, "br", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "client wants brotli"
    );
}

#[test]
fn compression_prefers_zstd_then_brotli_then_gzip() {
    assert_eq!(
        pick_encoding(2000, "gzip, br, zstd", 1024, "text/plain", &[]),
        Some(Encoding::Zstd),
        "zstd preferred when the client accepts all three"
    );
    assert_eq!(
        pick_encoding(2000, "gzip, br", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "brotli preferred over gzip when zstd isn't accepted"
    );
    assert_eq!(
        pick_encoding(2000, "deflate", 1024, "text/plain", &[]),
        None,
        "no supported encoding accepted"
    );
}

#[test]
fn compression_respects_weighted_accept_encoding() {
    // An explicit veto beats our own top preference.
    assert_eq!(
        pick_encoding(
            2000,
            "zstd;q=0, br;q=0.8, gzip;q=0.5",
            1024,
            "text/plain",
            &[]
        ),
        Some(Encoding::Brotli)
    );
    // A q value alone does not re-rank against our own priority order;
    // acceptable is all that is asked of it.
    assert_eq!(
        pick_encoding(2000, "zstd;q=0.1, gzip;q=1.0", 1024, "text/plain", &[]),
        Some(Encoding::Zstd)
    );

    assert_eq!(
        pick_encoding(2000, "*;q=1", 1024, "text/plain", &[]),
        Some(Encoding::Zstd)
    );
    // An explicit entry overrides the wildcard in either direction.
    assert_eq!(
        pick_encoding(2000, "*;q=1, zstd;q=0", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "explicit zstd;q=0 overrides the permissive wildcard"
    );
    assert_eq!(
        pick_encoding(2000, "*;q=0, gzip;q=1", 1024, "text/plain", &[]),
        Some(Encoding::Gzip),
        "explicit gzip;q=1 overrides the blanket wildcard veto"
    );
    assert_eq!(
        pick_encoding(2000, "*;q=0", 1024, "text/plain", &[]),
        None,
        "wildcard veto with no explicit overrides"
    );
}

#[test]
fn compression_mime_types_allowlist() {
    let allowed = vec!["text/html".to_string(), "application/json".to_string()];
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/html", &allowed),
        Some(Encoding::Gzip),
        "text/html is on the allowlist"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "image/png", &allowed),
        None,
        "image/png is not on the allowlist"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/html; charset=utf-8", &allowed),
        Some(Encoding::Gzip),
        "charset suffix must not defeat the match"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "image/png", &[]),
        Some(Encoding::Gzip),
        "empty allowlist (the default) means no restriction at all"
    );
}
