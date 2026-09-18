//! Wire types for the master <-> worker rings, postcard-encoded and handed
//! to `ipc::shm` as raw payloads; the ring owns the length prefix.
//!
//! Postcard never encodes to 0 bytes, so an empty payload is unambiguous and
//! doubles as the worker-done marker.

use crate::ipc::shm;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::Arc;
use tokio::io::unix::AsyncFd;

/// A large body streams to an unlinked temp file whose fd crosses over the
/// worker's body socket; `len` travels with it because PHP needs
/// CONTENT_LENGTH before `read_post()`, and stat()ing it would TOCTOU.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestBody<'a> {
    Inline(#[serde(borrow, with = "serde_bytes")] Cow<'a, [u8]>),
    File { len: u64 },
}

/// Every string field is a `Cow` so one type serves both directions: master
/// fills it with owned values, the worker decodes one borrowing straight out
/// of the ring scratch. Same wire bytes either way.
///
/// `#[serde(borrow)]` is needed on each field individually - serde does not
/// propagate it, and a field missing it silently allocates.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhpRequest<'a> {
    #[serde(borrow)]
    pub script_path: Cow<'a, str>,
    /// Already resolved against the matched target; php-mod has no notion of
    /// targets.
    #[serde(borrow)]
    pub document_root: Cow<'a, str>,
    #[serde(borrow)]
    pub script_name: Cow<'a, str>,
    /// Percent-decoded, unlike `uri`, so it cannot be a slice of it however
    /// much the two overlap.
    #[serde(borrow)]
    pub path_info: Cow<'a, str>,
    #[serde(borrow)]
    pub method: Cow<'a, str>,
    /// Path and query together, as PHP's REQUEST_URI. `QUERY_STRING` is not
    /// sent beside it: it is this same string past the first `?`, and hyper
    /// admits a URI large enough that carrying it twice matters.
    #[serde(borrow)]
    pub uri: Cow<'a, str>,
    /// Raw pairs as received; $_SERVER's HTTP_ mangling happens worker-side.
    #[serde(borrow)]
    pub headers: HeaderBlob<'a>,
    /// The resolved client, not the raw TCP peer.
    pub client_ip: std::net::IpAddr,
    #[serde(borrow)]
    pub body: RequestBody<'a>,
    /// Resolved master-side; php-mod knows nothing of listeners or proxy hops.
    #[serde(borrow)]
    pub server_name: Cow<'a, str>,
    pub server_addr: std::net::IpAddr,
    pub server_port: u16,
    #[serde(borrow)]
    pub server_protocol: Cow<'a, str>,
    pub https: bool,
}

/// Headers as one `name\0value\0…` blob, in both directions: postcard
/// rebuilds every `String` on decode, so a `Vec<(String, String)>` would
/// cost two allocations per header each way where this moves as one `memcpy`.
///
/// `\0` is a safe separator because hyper rejects NUL in header names and
/// values, and PHP's `header()` has rejected CR/LF/NUL since 5.1.2. `push`
/// rechecks anyway: a malformed entry would silently merge two headers.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderBlob<'a>(#[serde(borrow, with = "serde_bytes")] Cow<'a, [u8]>);

impl<'a> HeaderBlob<'a> {
    /// One upfront reservation is the point of this type; growing it header
    /// by header gives most of that back.
    pub fn with_capacity(byte_capacity: usize) -> Self {
        HeaderBlob(Cow::Owned(Vec::with_capacity(byte_capacity)))
    }

    /// Silently drops an entry containing a NUL: it cannot round-trip through
    /// this encoding, and admitting it would corrupt every following pair.
    pub fn push(&mut self, name: &str, value: &str) {
        if name.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return;
        }
        let buf = self.0.to_mut();
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(value.as_bytes());
        buf.push(0);
    }

    /// Stops at the first malformed pair rather than guessing: this crosses
    /// the master<->worker boundary, so a corrupt blob must degrade to fewer
    /// headers, never to a panic or a mis-paired name and value.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        let mut rest: &[u8] = &self.0;
        std::iter::from_fn(move || {
            let name_end = rest.iter().position(|&b| b == 0)?;
            let (name, after_name) = rest.split_at(name_end);
            let after_name = &after_name[1..];
            let value_end = after_name.iter().position(|&b| b == 0)?;
            let (value, after_value) = after_name.split_at(value_end);
            rest = &after_value[1..];
            match (std::str::from_utf8(name), std::str::from_utf8(value)) {
                (Ok(name), Ok(value)) => Some((name, value)),
                _ => None,
            }
        })
    }

    pub fn into_owned(self) -> HeaderBlob<'static> {
        HeaderBlob(Cow::Owned(self.0.into_owned()))
    }

    pub fn byte_len(&self) -> usize {
        self.0.len()
    }

    pub fn append(&mut self, other: &HeaderBlob<'_>) {
        self.0.to_mut().extend_from_slice(&other.0);
    }

    /// Pieces of at most `budget` bytes, cut only on entry boundaries. An
    /// entry larger than `budget` gets its own oversized piece, there being
    /// no smaller grouping for it.
    pub fn split_at_budget(&self, budget: usize) -> Vec<HeaderBlobRef<'_>> {
        let blob: &[u8] = &self.0;
        let mut pieces = Vec::new();
        let mut start = 0usize;
        // The only offset a cut may land on.
        let mut entry_start = 0usize;
        // Every second NUL closes an entry. Counting as we go keeps this to
        // one pass; re-scanning per NUL would be quadratic.
        let mut nuls = 0usize;
        for (i, &b) in blob.iter().enumerate() {
            if b != 0 {
                continue;
            }
            nuls += 1;
            if !nuls.is_multiple_of(2) {
                continue;
            }
            let entry_end = i + 1;
            // Cut only if something already fits, or an over-budget entry
            // would produce an empty piece ahead of itself.
            if entry_start > start && entry_end - start > budget {
                pieces.push(HeaderBlobRef(&blob[start..entry_start]));
                start = entry_start;
            }
            entry_start = entry_end;
        }
        if start < blob.len() || pieces.is_empty() {
            pieces.push(HeaderBlobRef(&blob[start..]));
        }
        pieces
    }
}

/// A borrowed slice of a [`HeaderBlob`], serializing identically to the
/// owned form.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct HeaderBlobRef<'a>(#[serde(with = "serde_bytes")] pub &'a [u8]);

impl Copy for HeaderBlobRef<'_> {}

impl Clone for HeaderBlobRef<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a> From<&'a HeaderBlob<'_>> for HeaderBlobRef<'a> {
    fn from(blob: &'a HeaderBlob<'_>) -> Self {
        HeaderBlobRef(&blob.0)
    }
}

/// One event in a worker's response. Ordered: one or more `Headers`, any
/// number of `Body`, exactly one `End`.
#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseFrame<'a> {
    /// A blob rather than a map, since header names may legitimately repeat.
    /// An oversized set splits across frames, `more` set on all but the last,
    /// so a reader can forward it without waiting for the run to end.
    Headers {
        status: u16,
        #[serde(borrow)]
        headers: HeaderBlob<'a>,
        more: bool,
    },
    Body(#[serde(borrow, with = "serde_bytes")] Cow<'a, [u8]>),
    /// The response is fully sent. Says nothing about the worker being free -
    /// after `fastcgi_finish_request()` the script keeps running - so callers
    /// must still wait for the worker-done marker.
    End {
        retiring: bool,
    },
}

impl ResponseFrame<'_> {
    /// The response path's one unavoidable copy: a decoded frame borrows
    /// ring scratch the next read overwrites, and a non-blocking sweep
    /// collects several frames before handing any of them on.
    pub fn into_owned(self) -> ResponseFrame<'static> {
        match self {
            ResponseFrame::Headers {
                status,
                headers,
                more,
            } => ResponseFrame::Headers {
                status,
                headers: headers.into_owned(),
                more,
            },
            ResponseFrame::Body(bytes) => ResponseFrame::Body(Cow::Owned(bytes.into_owned())),
            ResponseFrame::End { retiring } => ResponseFrame::End { retiring },
        }
    }
}

/// Write-side mirror of [`ResponseFrame`] that borrows its body, encoding
/// byte-identically. Lets the slice PHP hands `ub_write` go straight to
/// postcard instead of through a fresh `Vec`.
#[derive(Serialize)]
pub enum ResponseFrameRef<'a> {
    Headers {
        status: u16,
        headers: HeaderBlobRef<'a>,
        more: bool,
    },
    Body(#[serde(with = "serde_bytes")] &'a [u8]),
    End {
        retiring: bool,
    },
}

/// Encodes into `scratch`, reusing its allocation. The `mem::take` and
/// write-back are what let `to_extend` append into existing capacity, where
/// `to_allocvec` would malloc and free once per frame.
fn encode_into<'a, T: serde::Serialize + ?Sized>(
    scratch: &'a mut Vec<u8>,
    value: &T,
) -> std::io::Result<&'a [u8]> {
    let mut buf = std::mem::take(scratch);
    buf.clear();
    *scratch = postcard::to_extend(value, buf).map_err(to_io_err)?;
    Ok(scratch.as_slice())
}

/// One frame, always: the head cap master sets on hyper and this ring are
/// sized against each other, so no request hyper accepts can need two. One
/// that still does not fit is refused rather than split.
pub async fn write_request_to_ring(
    ring: &shm::RequestRing,
    peer: &shm::PeerDeath,
    encoded: &[u8],
    space_efd: &AsyncFd<OwnedFd>,
) -> std::io::Result<()> {
    ring.write_frame_async(encoded, peer, space_efd)
        .await
        .map_err(ring_err_to_io)
}

/// Worker side, blocking until a request arrives. `Ok(None)` means the peer
/// is gone - idle timeout is master's call, not this function's; see
/// `PoolManager::sweep_idle_workers`.
pub fn read_request_from_ring<'a>(
    ring: &shm::RequestRing,
    peer: &shm::PeerDeath,
    scratch: &'a mut Vec<u8>,
    notify_efd: RawFd,
) -> std::io::Result<Option<PhpRequest<'a>>> {
    match ring.read_frame_until(scratch, peer, notify_efd, None) {
        // Borrows `scratch`, so the request is valid until the next read. An
        // empty frame here (never written by the current request-ring
        // producer) falls through to postcard, which errors on it.
        Ok(_) => Ok(Some(postcard::from_bytes(scratch).map_err(to_io_err)?)),
        Err(shm::RingError::PeerGone) => Ok(None),
        Err(e) => Err(ring_err_to_io(e)),
    }
}

/// Worker side, blocking.
pub fn write_response_frame_to_ring(
    ring: &shm::ResponseRing,
    peer: &shm::PeerDeath,
    frame: &ResponseFrameRef<'_>,
    scratch: &mut Vec<u8>,
    notify_efd: RawFd,
) -> std::io::Result<()> {
    let bytes = encode_into(scratch, frame)?;
    ring.write_frame(bytes, peer, notify_efd)
        .map_err(ring_err_to_io)
}

/// Splits one `ub_write` chunk into as many `Body` frames as the ring's
/// capacity requires, each written straight from `bytes` so a large `echo`
/// pays two copies per byte rather than three.
pub fn write_body_to_ring(
    ring: &shm::ResponseRing,
    peer: &shm::PeerDeath,
    bytes: &[u8],
    chunk_size: usize,
    scratch: &mut Vec<u8>,
    notify_efd: RawFd,
) -> std::io::Result<()> {
    for sub in bytes.chunks(chunk_size) {
        write_response_frame_to_ring(
            ring,
            peer,
            &ResponseFrameRef::Body(sub),
            scratch,
            notify_efd,
        )?;
    }
    Ok(())
}

/// Splits across several frames only on `FrameTooLarge`, so the common case
/// costs one encode.
pub fn write_headers_to_ring(
    ring: &shm::ResponseRing,
    peer: &shm::PeerDeath,
    status: u16,
    headers: &HeaderBlob<'_>,
    scratch: &mut Vec<u8>,
    notify_efd: RawFd,
) -> std::io::Result<()> {
    let bytes = encode_into(
        scratch,
        &ResponseFrameRef::Headers {
            status,
            headers: headers.into(),
            more: false,
        },
    )?;
    match ring.write_frame(bytes, peer, notify_efd) {
        Ok(()) => Ok(()),
        Err(shm::RingError::FrameTooLarge) => {
            let pieces = split_headers_into_frames(headers);
            let last = pieces.len() - 1;
            for (i, piece) in pieces.into_iter().enumerate() {
                write_response_frame_to_ring(
                    ring,
                    peer,
                    &ResponseFrameRef::Headers {
                        status,
                        headers: piece,
                        more: i != last,
                    },
                    scratch,
                    notify_efd,
                )?;
            }
            Ok(())
        }
        Err(e) => Err(ring_err_to_io(e)),
    }
}

/// The budget leaves room for the frame's own postcard overhead on top of
/// the blob bytes.
fn split_headers_into_frames<'b>(headers: &'b HeaderBlob<'_>) -> Vec<HeaderBlobRef<'b>> {
    const CHUNK_BUDGET: usize = shm::RESPONSE_RING_CAPACITY / 2;
    headers.split_at_budget(CHUNK_BUDGET)
}

/// Sent once `execute_file` truly returns, which after
/// `fastcgi_finish_request()` can be long after `End`. The only signal that
/// the worker is free.
pub fn write_worker_done_to_ring(
    ring: &shm::ResponseRing,
    peer: &shm::PeerDeath,
    notify_efd: RawFd,
) -> std::io::Result<()> {
    ring.write_frame(&[], peer, notify_efd)
        .map_err(ring_err_to_io)
}

/// Fixed bounds rather than a decaying high-water mark: a buffer that grew
/// this far served an outlier, and optimising for its recurrence is the wrong
/// bet.
const SCRATCH_SHRINK_ABOVE: usize = 64 * 1024;
const SCRATCH_KEEP_CAPACITY: usize = 4 * 1024;

/// Hands back capacity a single large request left behind; these buffers are
/// otherwise only ever cleared, so the peak is pinned for the worker's life.
///
/// Only where nothing borrows `buf`: a decoded frame borrows its scratch.
pub fn shrink_scratch(buf: &mut Vec<u8>) {
    if buf.capacity() > SCRATCH_SHRINK_ABOVE {
        buf.clear();
        buf.shrink_to(SCRATCH_KEEP_CAPACITY);
    }
}

/// Reused for a worker's whole life, so a steady-state request allocates in
/// neither direction. Grouped rather than passed as two loose buffers, which
/// would be easy to transpose at a call site.
#[derive(Default)]
pub struct RingScratch {
    pub read: Vec<u8>,
    pub write: Vec<u8>,
}

impl RingScratch {
    /// Between requests only, never while a decoded command borrows `read`.
    pub fn shrink(&mut self) {
        shrink_scratch(&mut self.read);
        shrink_scratch(&mut self.write);
    }
}

pub fn encode_request<'a>(
    scratch: &'a mut Vec<u8>,
    req: &PhpRequest<'_>,
) -> std::io::Result<&'a [u8]> {
    encode_into(scratch, req)
}

/// What a non-blocking read of the response ring found. `Empty` is only ever
/// "nothing published yet"; end of stream is `WorkerDone`.
pub enum ReadyResponse {
    Frame(ResponseFrame<'static>),
    WorkerDone,
    Empty,
}

/// Master side, non-blocking. Reports the worker-done marker without
/// reclaiming on it, which `read_response_frame_from_ring` does and this
/// cannot: reclaim has to be awaited.
pub fn try_read_response_frame_from_ring(
    mapped: &Arc<shm::MappedChannel>,
    scratch: &mut Vec<u8>,
) -> std::io::Result<ReadyResponse> {
    let channel = mapped.channel();
    if !channel
        .response
        .try_read_frame(scratch, &channel.peer_death)
        .map_err(ring_err_to_io)?
    {
        return Ok(ReadyResponse::Empty);
    }
    if scratch.is_empty() {
        return Ok(ReadyResponse::WorkerDone);
    }
    let frame: ResponseFrame<'_> = postcard::from_bytes(scratch).map_err(to_io_err)?;
    Ok(ReadyResponse::Frame(frame.into_owned()))
}

/// Master side, async. `Ok(None)` is the trailing worker-done marker.
///
/// Reclaiming the rings here was tried and abandoned: the punched pages fault
/// straight back in on the worker about to reuse them.
pub async fn read_response_frame_from_ring(
    mapped: &Arc<shm::MappedChannel>,
    scratch: &mut Vec<u8>,
    data_efd: &AsyncFd<OwnedFd>,
) -> std::io::Result<Option<ResponseFrame<'static>>> {
    let channel = mapped.channel();
    channel
        .response
        .read_frame_async(scratch, &channel.peer_death, data_efd)
        .await
        .map_err(ring_err_to_io)?;
    if scratch.is_empty() {
        return Ok(None);
    }
    let frame: ResponseFrame<'_> = postcard::from_bytes(scratch).map_err(to_io_err)?;
    Ok(Some(frame.into_owned()))
}

pub(crate) fn ring_err_to_io(e: shm::RingError) -> std::io::Error {
    match e {
        // `InvalidInput`, not `InvalidData`: what is wrong is the frame we
        // were asked to send, not anything the peer produced. Callers tell a
        // request they must refuse from a worker they must replace by this.
        shm::RingError::FrameTooLarge => std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame exceeds ring capacity",
        ),
        shm::RingError::PeerGone => {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer process is gone")
        }
        shm::RingError::Truncated => std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer published a frame header without its payload",
        ),
    }
}

pub(crate) fn to_io_err(e: postcard::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}

#[cfg(test)]
#[path = "data_tests.rs"]
mod tests;
