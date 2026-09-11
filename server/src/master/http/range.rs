//! Single-range `Range`/`Content-Range` parsing, and the stream every static
//! file body is read through, whole file or one range, compressed or not.

use crate::worker::COALESCE_FLUSH_THRESHOLD;
use bytes::Bytes;
use std::future::Future;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::FileExt as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
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
    let end: Option<u64> = if end_s.is_empty() {
        None
    } else {
        Some(end_s.parse().ok()?)
    };
    if len == 0 || start >= len {
        return Some(Err(()));
    }
    let end = end.unwrap_or(len - 1).min(len - 1);
    if end < start {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}

/// Cleared for the rest of the process the first time the kernel or the
/// filesystem rejects the flag: that answer cannot change under a running
/// server, and retrying would spend a syscall per chunk forever.
static NOWAIT_USABLE: AtomicBool = AtomicBool::new(true);

/// Caps how many chunks one poll may serve straight from page cache: a fully
/// cached large file would otherwise hold a runtime worker for its whole
/// length, nothing in the fast path being an await point.
const MAX_SYNC_CHUNKS: u8 = 8;

/// `pread` that reports `EAGAIN` instead of waiting when the data is not
/// already in page cache, so a hit can be served without a trip through the
/// blocking pool.
fn pread_nowait(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    let iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // The offset is split because the kernel rebuilds it as
    // `(pos_h << 32) | pos_l`, and widened because every argument but the
    // flags is an `unsigned long` there.
    let n = unsafe {
        libc::syscall(
            libc::SYS_preadv2,
            file.as_raw_fd() as libc::c_long,
            &iov as *const libc::iovec,
            1 as libc::c_long,
            (offset & 0xffff_ffff) as libc::c_long,
            (offset >> 32) as libc::c_long,
            libc::RWF_NOWAIT,
        )
    };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// The flag not being implemented at all, as opposed to this one read not
/// being servable without waiting. XFS also answers `EAGAIN` when it cannot
/// take the inode lock, so `EAGAIN` says nothing about page cache either.
fn nowait_unsupported(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL)
    )
}

/// Either the fd is ours to read from or a blocking read has it, never both
/// and never neither while the stream is still live.
enum ReadState {
    Idle(std::fs::File),
    Reading(tokio::task::JoinHandle<(std::fs::File, std::io::Result<Bytes>)>),
    /// Stands in while the fd is moved out, and remains only once the stream
    /// has failed - which releases the fd with it.
    Done,
}

/// Streams `len` bytes from `offset`, page cache directly where it can and
/// through the blocking pool where it must.
///
/// `pread` rather than seek-then-read throughout: a seek would be its own
/// blocking-pool round trip where the offset argument costs none.
pub(crate) struct FileBody {
    state: ReadState,
    offset: u64,
    remaining: u64,
    sync_chunks: u8,
}

impl FileBody {
    pub(crate) fn new(file: std::fs::File, offset: u64, len: u64) -> Self {
        FileBody {
            state: ReadState::Idle(file),
            offset,
            remaining: len,
            sync_chunks: 0,
        }
    }

    /// Nothing left to read before `remaining` runs out means the file shrank
    /// under a length already promised in the response headers, so the stream
    /// fails rather than truncating silently. A short read just advances.
    fn advance(&mut self, bytes: Bytes) -> Option<std::io::Result<Bytes>> {
        if bytes.is_empty() {
            self.remaining = 0;
            return Some(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "file shrank during read",
            )));
        }
        self.offset += bytes.len() as u64;
        self.remaining = self.remaining.saturating_sub(bytes.len() as u64);
        Some(Ok(bytes))
    }

    fn fail(&mut self, e: std::io::Error) -> Poll<Option<std::io::Result<Bytes>>> {
        self.remaining = 0;
        self.state = ReadState::Done;
        Poll::Ready(Some(Err(e)))
    }
}

impl Stream for FileBody {
    type Item = std::io::Result<Bytes>;

    /// Loops at most twice: issuing a blocking read falls through to polling
    /// it, so the task's waker is registered before this returns.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.remaining == 0 {
                return Poll::Ready(None);
            }
            let file = match std::mem::replace(&mut this.state, ReadState::Done) {
                ReadState::Done => return Poll::Ready(None),
                ReadState::Reading(mut task) => {
                    let (file, result) = match Pin::new(&mut task).poll(cx) {
                        Poll::Ready(res) => res.expect("blocking task panicked"),
                        Poll::Pending => {
                            this.state = ReadState::Reading(task);
                            return Poll::Pending;
                        }
                    };
                    return match result {
                        Ok(bytes) => {
                            this.state = ReadState::Idle(file);
                            Poll::Ready(this.advance(bytes))
                        }
                        Err(e) => this.fail(e),
                    };
                }
                ReadState::Idle(file) => file,
            };

            let offset = this.offset;
            let chunk_len = this.remaining.min(COALESCE_FLUSH_THRESHOLD as u64) as usize;

            if this.sync_chunks >= MAX_SYNC_CHUNKS {
                this.sync_chunks = 0;
                this.state = ReadState::Idle(file);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            // Moved into the fallback below if the fast path cannot serve
            // this chunk, rather than allocated twice.
            let mut buf = vec![0u8; chunk_len];
            if NOWAIT_USABLE.load(Relaxed) {
                match pread_nowait(&file, &mut buf, offset) {
                    Ok(n) => {
                        buf.truncate(n);
                        this.state = ReadState::Idle(file);
                        this.sync_chunks += 1;
                        return Poll::Ready(this.advance(Bytes::from(buf)));
                    }
                    Err(e) if nowait_unsupported(&e) => NOWAIT_USABLE.store(false, Relaxed),
                    Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {}
                    Err(e) => return this.fail(e),
                }
            }

            this.sync_chunks = 0;
            this.state = ReadState::Reading(tokio::task::spawn_blocking(move || {
                let result = file.read_at(&mut buf, offset).map(|n| {
                    buf.truncate(n);
                    Bytes::from(buf)
                });
                (file, result)
            }));
        }
    }
}

#[cfg(test)]
#[path = "range_tests.rs"]
mod tests;
