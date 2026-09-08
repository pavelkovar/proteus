//! Single-range `Range`/`Content-Range`: parsing the header and streaming
//! the selected slice. Always identity bytes, never compressed.

use crate::worker::COALESCE_FLUSH_THRESHOLD;
use bytes::Bytes;
use std::future::Future;
use std::os::unix::fs::FileExt as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

/// Single-range only (RFC 9110 §14.1.2). `None` ignores `Range` entirely;
/// `Some(Err(()))` is well-formed but unsatisfiable, which the caller turns
/// into a 416.
///
/// The empty-file check comes after parsing, so a malformed header does not
/// get a different outcome depending on the file it is applied to.
pub(crate) fn parse_range(range: &str, len: u64) -> Option<Result<(u64, u64), ()>> {
    let spec = range.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (start_s, end_s) = spec.split_once('-')?;
    if start_s.is_empty() {
        let suffix_len: u64 = end_s.parse().ok()?;
        if suffix_len == 0 || len == 0 {
            return Some(Err(()));
        }
        let suffix_len = suffix_len.min(len);
        return Some(Ok((len - suffix_len, len - 1)));
    }
    let start: u64 = start_s.parse().ok()?;
    let end: Option<u64> = if end_s.is_empty() { None } else { Some(end_s.parse().ok()?) };
    if len == 0 || start >= len {
        return Some(Err(()));
    }
    let end = end.unwrap_or(len - 1).min(len - 1);
    if end < start {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}

/// Uses `pread` rather than seek-then-read: `tokio::fs::File` dispatches a
/// seek as its own `spawn_blocking` call, so that would cost two
/// blocking-pool round trips where the offset argument does it in one.
pub(crate) struct RangeBody {
    file: Option<std::fs::File>,
    task: Option<tokio::task::JoinHandle<(std::fs::File, std::io::Result<Bytes>)>>,
    offset: u64,
    remaining: u64,
}

impl RangeBody {
    pub(crate) fn new(file: std::fs::File, offset: u64, len: u64) -> Self {
        RangeBody { file: Some(file), task: None, offset, remaining: len }
    }
}

impl Stream for RangeBody {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        if this.task.is_none() {
            let file = this.file.take().expect("RangeBody: file missing while idle");
            let offset = this.offset;
            let chunk_len = this.remaining.min(COALESCE_FLUSH_THRESHOLD as u64) as usize;
            this.task = Some(tokio::task::spawn_blocking(move || {
                let mut buf = vec![0u8; chunk_len];
                let result = file.read_at(&mut buf, offset).map(|n| {
                    buf.truncate(n);
                    Bytes::from(buf)
                });
                (file, result)
            }));
        }
        let (file, result) = match Pin::new(this.task.as_mut().unwrap()).poll(cx) {
            Poll::Ready(res) => res.expect("blocking task panicked"),
            Poll::Pending => return Poll::Pending,
        };
        this.task = None;
        this.file = Some(file);
        match result {
            Ok(bytes) if bytes.is_empty() => {
                this.remaining = 0;
                Poll::Ready(Some(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "file shrank during range read"))))
            }
            Ok(bytes) => {
                this.offset += bytes.len() as u64;
                this.remaining = this.remaining.saturating_sub(bytes.len() as u64);
                Poll::Ready(Some(Ok(bytes)))
            }
            Err(e) => {
                this.remaining = 0;
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

#[cfg(test)]
#[path = "range_tests.rs"]
mod tests;
