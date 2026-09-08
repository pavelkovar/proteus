//! Accept-Encoding negotiation and the sync streaming encoders.

use super::ResponseBody;
use bytes::{BufMut as _, Bytes, BytesMut};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use std::io::Write as _;
use tokio_stream::{Stream, StreamExt as _};
use tokio_stream::wrappers::ReceiverStream;

/// Caps how much of a bursty `source` is folded into one encode round
/// trip, trading total-time overhead against time-to-first-byte.
const COMPRESSION_COALESCE_THRESHOLD: usize = 256 * 1024;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Encoding {
    Brotli,
    Zstd,
    Gzip,
}

impl Encoding {
    fn header_value(self) -> &'static str {
        match self {
            Encoding::Brotli => "br",
            Encoding::Zstd => "zstd",
            Encoding::Gzip => "gzip",
        }
    }

    /// zstd drops a level when the size is unknown: that is the one case
    /// where the window can't be trimmed to the body, so the level is the
    /// only lever left on a live encoder's memory.
    fn level(self, size_hint: Option<u64>) -> i32 {
        match self {
            Encoding::Brotli => 4,
            Encoding::Zstd if size_hint.is_none() => 1,
            Encoding::Zstd => 3,
            Encoding::Gzip => 3,
        }
    }
}

/// Which codings a client will accept. Fixed fields rather than a parsed
/// collection, to keep negotiation allocation-free; `None` is "not
/// mentioned", distinct from an explicit `q=0` because RFC 9110 §12.5.3
/// lets either override a `*` wildcard.
#[derive(Default)]
struct AcceptedEncodings {
    zstd: Option<bool>,
    brotli: Option<bool>,
    gzip: Option<bool>,
    star: Option<bool>,
}

impl AcceptedEncodings {
    fn parse(accept_encoding: &str) -> Self {
        let mut out = AcceptedEncodings::default();
        for entry in accept_encoding.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let mut parts = entry.split(';');
            let Some(coding) = parts.next().map(str::trim) else {
                continue;
            };
            let q = parts
                .find_map(|p| p.trim().strip_prefix("q="))
                .and_then(|v| v.trim().parse::<f32>().ok())
                .unwrap_or(1.0);
            let acceptable = q > 0.0;
            let slot = if coding.eq_ignore_ascii_case("zstd") {
                &mut out.zstd
            } else if coding.eq_ignore_ascii_case("br") {
                &mut out.brotli
            } else if coding.eq_ignore_ascii_case("gzip") {
                &mut out.gzip
            } else if coding == "*" {
                &mut out.star
            } else {
                continue;
            };
            slot.get_or_insert(acceptable);
        }
        out
    }

    /// An exact match wins over `*`; unmentioned is unacceptable, per spec.
    fn accepts(&self, encoding: Encoding) -> bool {
        let explicit = match encoding {
            Encoding::Zstd => self.zstd,
            Encoding::Brotli => self.brotli,
            Encoding::Gzip => self.gzip,
        };
        explicit.or(self.star).unwrap_or(false)
    }
}

/// Whether a body may be compressed at all, independent of what the client
/// accepts - so it can also answer whether to send `Vary: Accept-Encoding`.
/// An empty `mime_types` means no restriction.
pub(crate) fn compression_eligible(body_len: usize, min_size_bytes: usize, content_type: &str, mime_types: &[String]) -> bool {
    if body_len < min_size_bytes {
        return false;
    }
    if mime_types.is_empty() {
        return true;
    }
    let base_type = content_type.split(';').next().unwrap_or("").trim();
    mime_types.iter().any(|m| m.eq_ignore_ascii_case(base_type))
}

#[cfg(test)]
pub(crate) fn pick_encoding(
    body_len: usize,
    accept_encoding: &str,
    min_size_bytes: usize,
    content_type: &str,
    mime_types: &[String],
) -> Option<Encoding> {
    pick_encoding_when_eligible(
        compression_eligible(body_len, min_size_bytes, content_type, mime_types),
        accept_encoding,
    )
}

/// Picks the server's most-preferred acceptable coding - a server-side
/// priority list gated by client vetoes, not the client's highest `q`.
/// Takes `eligible` already computed, since `Vary` needs the same answer.
pub(crate) fn pick_encoding_when_eligible(eligible: bool, accept_encoding: &str) -> Option<Encoding> {
    if !eligible {
        return None;
    }
    let accepted = AcceptedEncodings::parse(accept_encoding);
    [Encoding::Zstd, Encoding::Brotli, Encoding::Gzip].into_iter().find(|&e| accepted.accepts(e))
}

/// The `(body_len, min_size_bytes)` pair to gate a streamed response on.
///
/// Without a declared length the minimum-size gate is disabled rather than
/// buffered for, since finding the size out would cost time-to-first-byte.
/// `declared_len` is a hint only: never used for framing, so a wrong value
/// can mis-pick compression but cannot corrupt the response.
pub(crate) fn stream_size_gate(declared_len: Option<usize>, min_size_bytes: usize) -> (usize, usize) {
    match declared_len {
        Some(len) => (len, min_size_bytes),
        None => (usize::MAX, 0),
    }
}

pub(crate) fn with_content_encoding(builder: hyper::http::response::Builder, encoding: Encoding) -> hyper::http::response::Builder {
    builder.header(hyper::header::CONTENT_ENCODING, encoding.header_value())
}

/// A streaming encoder. CPU-bound and blocking; never drive it from a
/// tokio worker thread.
enum SyncEncoder {
    Brotli(brotli::CompressorWriter<bytes::buf::Writer<BytesMut>>),
    Zstd(zstd::stream::write::Encoder<'static, bytes::buf::Writer<BytesMut>>),
    Gzip(flate2::write::GzEncoder<bytes::buf::Writer<BytesMut>>),
}

const INITIAL_SINK_CAPACITY: usize = 8 * 1024;

/// Ceiling on brotli `lgwin` / zstd `WindowLog`, bounding a live encoder's
/// resident memory. Gzip has no equivalent; DEFLATE's window is fixed by spec.
const WINDOW_LOG_MAX: u32 = 18;

/// zstd rejects a `WindowLog` below this.
const WINDOW_LOG_MIN: u32 = 10;

/// The window to give an encoder for a body of `size_hint` bytes, rounded
/// up to a power of two - a window wider than the body costs memory and
/// buys no ratio.
///
/// Safe to drive from an unverified script `Content-Length`, because the
/// window affects ratio and nothing else. `set_pledged_src_size` is not:
/// it is a contract, and breaking it fails the encode mid-body, long after
/// the headers have gone out.
fn window_log_for(size_hint: Option<u64>) -> u32 {
    let Some(len) = size_hint else {
        return WINDOW_LOG_MAX;
    };
    let bits = u64::BITS - len.max(1).leading_zeros();
    bits.clamp(WINDOW_LOG_MIN, WINDOW_LOG_MAX)
}

impl SyncEncoder {
    /// `size_hint` is `None` for a body whose size cannot be known before
    /// the headers go out.
    fn new(encoding: Encoding, size_hint: Option<u64>) -> Self {
        let level = encoding.level(size_hint);
        let window_log = window_log_for(size_hint);
        match encoding {
            Encoding::Brotli => SyncEncoder::Brotli(brotli::CompressorWriter::new(
                BytesMut::with_capacity(INITIAL_SINK_CAPACITY).writer(),
                4096,
                level as u32,
                window_log,
            )),
            Encoding::Zstd => {
                let mut enc = zstd::stream::write::Encoder::new(BytesMut::with_capacity(INITIAL_SINK_CAPACITY).writer(), level)
                    .expect("zstd encoder init is infallible for an in-memory sink");
                enc.set_parameter(zstd::stream::raw::CParameter::WindowLog(window_log))
                    .expect("WindowLog is a valid zstd parameter at this encoder stage");
                SyncEncoder::Zstd(enc)
            }
            Encoding::Gzip => SyncEncoder::Gzip(flate2::write::GzEncoder::new(
                BytesMut::with_capacity(INITIAL_SINK_CAPACITY).writer(),
                flate2::Compression::new(level as u32),
            )),
        }
    }

    /// Produces no output of its own; the bytes may sit in the encoder's
    /// buffer until `flush_and_drain` or `finish`.
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        match self {
            SyncEncoder::Brotli(w) => w.write_all(chunk),
            SyncEncoder::Zstd(w) => w.write_all(chunk),
            SyncEncoder::Gzip(w) => w.write_all(chunk),
        }
    }

    fn flush_and_drain(&mut self) -> std::io::Result<Bytes> {
        let sink = match self {
            SyncEncoder::Brotli(w) => {
                w.flush()?;
                w.get_mut()
            }
            SyncEncoder::Zstd(w) => {
                w.flush()?;
                w.get_mut()
            }
            SyncEncoder::Gzip(w) => {
                w.flush()?;
                w.get_mut()
            }
        };
        Ok(sink.get_mut().split().freeze())
    }

    fn finish(self) -> std::io::Result<Bytes> {
        match self {
            // brotli's `into_inner` runs BROTLI_OPERATION_FINISH itself and
            // swallows its error, so there is nothing left to propagate.
            SyncEncoder::Brotli(w) => Ok(w.into_inner().into_inner().freeze()),
            SyncEncoder::Zstd(w) => Ok(w.finish()?.into_inner().freeze()),
            SyncEncoder::Gzip(w) => Ok(w.finish()?.into_inner().freeze()),
        }
    }
}

/// Polls with a no-op waker: `Pending` means "nothing ready right now",
/// and nothing will be woken for it.
fn poll_next_now<S: Stream<Item = std::io::Result<Bytes>>>(
    source: std::pin::Pin<&mut S>,
) -> std::task::Poll<Option<std::io::Result<Bytes>>> {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    source.poll_next(&mut cx)
}

/// Compresses `source` chunk-by-chunk, holding a blocking-pool thread only
/// for each encode call.
///
/// Do not hoist the loop into one `spawn_blocking` per response: `source`
/// may itself need the blocking pool, and a slow client would pin the
/// thread for the length of the download - together enough to starve the
/// pool for every other user in the process.
pub(crate) fn compressed_body<S>(source: S, encoding: Encoding, size_hint: Option<u64>) -> ResponseBody
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    let (out_tx, out_rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(4);

    tokio::spawn(async move {
        tokio::pin!(source);
        let mut encoder = SyncEncoder::new(encoding, size_hint);

        loop {
            let first = match source.next().await {
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => {
                    let _ = out_tx.send(Err(e)).await;
                    return;
                }
                None => break,
            };
            let mut coalesced_len = first.len();
            let mut pending = vec![first];

            // Capped: without it a source that is always ready would defer
            // the first flush indefinitely.
            let mut source_done = false;
            while coalesced_len < COMPRESSION_COALESCE_THRESHOLD {
                match poll_next_now(source.as_mut()) {
                    std::task::Poll::Ready(Some(Ok(bytes))) => {
                        coalesced_len += bytes.len();
                        pending.push(bytes);
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        let _ = out_tx.send(Err(e)).await;
                        return;
                    }
                    std::task::Poll::Ready(None) => {
                        source_done = true;
                        break;
                    }
                    std::task::Poll::Pending => break,
                }
            }

            // `encoder` moves into the closure and back out, because it has
            // to survive into the next batch.
            let encode = move || -> std::io::Result<(SyncEncoder, Bytes)> {
                for chunk in &pending {
                    encoder.write(chunk)?;
                }
                let produced = encoder.flush_and_drain()?;
                Ok((encoder, produced))
            };
            match tokio::task::spawn_blocking(encode).await.expect("compression task panicked") {
                Ok((enc, produced)) => {
                    encoder = enc;
                    if !produced.is_empty() && out_tx.send(Ok(produced)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = out_tx.send(Err(e)).await;
                    return;
                }
            }

            if source_done {
                break;
            }
        }

        match tokio::task::spawn_blocking(move || encoder.finish()).await.expect("compression task panicked") {
            Ok(tail) if !tail.is_empty() => {
                let _ = out_tx.send(Ok(tail)).await;
            }
            Ok(_) => {}
            Err(e) => {
                let _ = out_tx.send(Err(e)).await;
            }
        }
    });

    body_from_stream(ReceiverStream::new(out_rx))
}

pub(crate) fn body_from_stream<S>(stream: S) -> ResponseBody
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    StreamBody::new(stream.map(|r| r.map(Frame::data))).boxed()
}

#[cfg(test)]
#[path = "compression_tests.rs"]
mod tests;
