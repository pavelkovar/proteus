//! Master-side bridge from the ring protocol onto async. Ring calls run on
//! the caller's own task and park on an eventfd, so no tokio worker thread
//! ever blocks on a futex.

use super::pool_manager;
use crate::ipc::control::WorkerReadyFds;
use crate::ipc::data::{self, PhpRequest, ReadyResponse, ResponseFrame};
use crate::ipc::shm;
use bytes::Bytes;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncReadExt;
use tokio::io::unix::AsyncFd;
use tokio::net::UnixStream as TokioUnixStream;

/// Bounds one run of `Headers` frames. The run length is worker-controlled,
/// so leaving it unbounded is an amplification risk against a shared process,
/// not merely a memory one.
const MAX_PENDING_HEADERS_BYTES: usize = 16 * 1024 * 1024;

/// One pooled worker's data channel, reused for that worker's whole life.
///
/// Every method drives the ring on the calling task, so a caller's timeout
/// may drop one mid-flight: none publishes half a request or consumes half a
/// frame, and a partly-joined `Headers` run resumes on the next read.
pub struct WorkerChannel {
    pid: u32,
    mapped: Arc<shm::MappedChannel>,
    req_space_efd: AsyncFd<OwnedFd>,
    resp_data_efd: AsyncFd<OwnedFd>,
    /// Reused for the worker's whole life, so a steady-state request neither
    /// encodes nor decodes with an allocation.
    read_scratch: Vec<u8>,
    encode_scratch: Vec<u8>,
    /// A run of `Headers` frames, joined so nothing downstream sees the
    /// split. `None` means no run is in progress.
    pending_headers: Option<(u16, data::HeaderBlob<'static>)>,
    pending_headers_bytes: usize,
    /// The event that flushing a headers run displaced, held so the next read
    /// hands it out in the order the worker wrote it. `Some(None)` is the
    /// worker-done marker.
    deferred: Option<Option<WorkerEvent>>,
    /// A plain flag rather than a `watch`: nothing awaits it, and it is read
    /// on every pop from the idle pool, where an atomic load beats
    /// `watch::Receiver::borrow` taking an internal read lock.
    worker_gone: Arc<AtomicBool>,
}

/// One response event, as the rest of master sees it. Distinct from the wire
/// `ResponseFrame`, whose `more` flag describes a split this side has already
/// joined back together and must not be able to express afterwards.
#[derive(Debug)]
pub enum WorkerEvent {
    Headers {
        status: u16,
        headers: data::HeaderBlob<'static>,
    },
    Body(Bytes),
    End {
        retiring: bool,
    },
}

/// One raw ring frame, after the headers-run accumulator has seen it.
enum Absorbed {
    /// Hand this to the caller; `None` is the worker-done marker.
    Ready(Option<WorkerEvent>),
    /// Joined onto a run still in progress, so read again.
    Folded,
}

/// Dropping a `WorkerChannel` means master is giving up on the worker; the
/// pool never drops one it means to reuse. Without this signal the worker
/// would park forever in its untimed ring wait, holding its PHP heap and
/// OPcache mapping resident with nothing left to notice.
///
/// A signal-free wake rather than `SIGKILL`, so the worker exits through its
/// own shutdown and runs PHP's shutdown functions. Killing unconditionally
/// here would also race a pid this process no longer owns.
impl Drop for WorkerChannel {
    fn drop(&mut self) {
        self.mapped.channel().mark_peer_dead();
    }
}

impl WorkerChannel {
    /// Spawns the liveness watcher, torn down on drop.
    pub fn new(fds: WorkerReadyFds, pid: u32) -> std::io::Result<Self> {
        let WorkerReadyFds {
            channel: channel_fd,
            liveness: liveness_fd,
            notify,
        } = fds;
        let mapped = Arc::new(shm::map_existing_channel(channel_fd)?);

        // Its own dup'd fds, so it can never notify through a number this
        // channel has closed and the OS has reused.
        let watcher_notify = notify.try_clone()?;
        let req_space_efd = AsyncFd::new(notify.req_space)?;
        let resp_data_efd = AsyncFd::new(notify.resp_data)?;

        let worker_gone = Arc::new(AtomicBool::new(false));
        spawn_liveness_watcher(
            liveness_fd,
            Arc::clone(&mapped),
            Arc::clone(&worker_gone),
            watcher_notify,
        )?;

        Ok(WorkerChannel {
            pid,
            mapped,
            req_space_efd,
            resp_data_efd,
            read_scratch: Vec::new(),
            encode_scratch: Vec::new(),
            pending_headers: None,
            pending_headers_bytes: 0,
            deferred: None,
            worker_gone,
        })
    }

    /// Bypasses the liveness watcher, so `peer.is_dead()` never becomes true
    /// on its own whatever `pid` is.
    #[cfg(test)]
    pub(crate) fn for_test(
        pid: u32,
        mapped: Arc<shm::MappedChannel>,
        req_space_efd: AsyncFd<OwnedFd>,
        resp_data_efd: AsyncFd<OwnedFd>,
    ) -> Self {
        WorkerChannel {
            pid,
            mapped,
            req_space_efd,
            resp_data_efd,
            read_scratch: Vec::new(),
            encode_scratch: Vec::new(),
            pending_headers: None,
            pending_headers_bytes: 0,
            deferred: None,
            worker_gone: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Publishes one request and resets the per-response state. Waits only
    /// for ring space before writing in one step, so a caller's timeout
    /// firing here leaves the worker nothing to read.
    pub async fn write_request(&mut self, req: &PhpRequest<'_>) -> std::io::Result<()> {
        self.pending_headers = None;
        self.pending_headers_bytes = 0;
        self.deferred = None;

        let Self {
            mapped,
            encode_scratch,
            req_space_efd,
            ..
        } = self;
        let channel = mapped.channel();
        let encoded = data::encode_request(encode_scratch, req)?;
        data::write_request_to_ring(
            &channel.request,
            &channel.peer_death,
            encoded,
            req_space_efd,
        )
        .await
    }

    /// The next frame if the worker has already published one. `None` means
    /// nothing has arrived yet, never end of stream - a caller that treats it
    /// as EOF would truncate the response.
    pub fn try_read_response_frame(&mut self) -> Option<std::io::Result<WorkerEvent>> {
        if let Some(ready) = self.deferred.take() {
            return Some(ready.ok_or_else(unexpected_marker));
        }
        loop {
            let Self {
                mapped,
                read_scratch,
                ..
            } = self;
            let raw = match data::try_read_response_frame_from_ring(mapped, read_scratch) {
                Ok(ReadyResponse::Frame(frame)) => Some(frame),
                Ok(ReadyResponse::WorkerDone) => None,
                Ok(ReadyResponse::Empty) => return None,
                Err(e) => return Some(Err(e)),
            };
            match self.absorb(raw) {
                Ok(Absorbed::Ready(frame)) => {
                    return Some(frame.ok_or_else(unexpected_marker));
                }
                Ok(Absorbed::Folded) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }

    /// The next event. Callers loop until `End`.
    pub async fn read_response_frame(&mut self) -> std::io::Result<WorkerEvent> {
        self.next_frame().await?.ok_or_else(unexpected_marker)
    }

    /// A real event arriving here means the wire desynced, which is a hard
    /// error rather than something to swallow.
    pub async fn read_worker_done(&mut self) -> std::io::Result<()> {
        match self.next_frame().await? {
            None => Ok(()),
            Some(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected the worker-done marker, got a response frame instead",
            )),
        }
    }

    /// `Ok(None)` is the trailing worker-done marker.
    async fn next_frame(&mut self) -> std::io::Result<Option<WorkerEvent>> {
        if let Some(ready) = self.deferred.take() {
            return Ok(ready);
        }
        loop {
            let Self {
                mapped,
                read_scratch,
                resp_data_efd,
                ..
            } = self;
            let raw =
                data::read_response_frame_from_ring(mapped, read_scratch, resp_data_efd).await?;
            if let Absorbed::Ready(frame) = self.absorb(raw)? {
                return Ok(frame);
            }
        }
    }

    /// Joins a run of `Headers` frames back together, forwarding on the run's
    /// last frame rather than waiting for the next one to imply the run
    /// ended: a script whose `header()` calls and first output are not
    /// back-to-back would otherwise pay a round-trip of TTFB.
    fn absorb(&mut self, raw: Option<ResponseFrame<'static>>) -> std::io::Result<Absorbed> {
        let Some(ResponseFrame::Headers {
            status,
            headers,
            more,
        }) = raw
        else {
            let displaced = raw.map(|frame| match frame {
                ResponseFrame::Body(chunk) => WorkerEvent::Body(Bytes::from(chunk.into_owned())),
                ResponseFrame::End { retiring } => WorkerEvent::End { retiring },
                ResponseFrame::Headers { .. } => unreachable!("matched above"),
            });
            // A worker that fails mid-run still writes the done marker
            // straight past End; without this flush its collected headers
            // would be silently dropped.
            let Some((status, headers)) = self.pending_headers.take() else {
                return Ok(Absorbed::Ready(displaced));
            };
            self.deferred = Some(displaced);
            return Ok(Absorbed::Ready(Some(WorkerEvent::Headers {
                status,
                headers,
            })));
        };

        self.pending_headers_bytes += self.read_scratch.len();
        if self.pending_headers_bytes > MAX_PENDING_HEADERS_BYTES {
            // Abandoning the ring is not enough: the worker would park
            // forever in its untimed wait_for_space, whose only other escape
            // is real process death.
            pool_manager::sigkill(self.pid, "worker sent an oversized run of Headers frames");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "worker sent an oversized run of Headers frames",
            ));
        }
        match &mut self.pending_headers {
            Some((_, acc)) => acc.append(&headers),
            None => self.pending_headers = Some((status, headers)),
        }
        if more {
            return Ok(Absorbed::Folded);
        }
        let (status, headers) = self.pending_headers.take().expect("just inserted above");
        Ok(Absorbed::Ready(Some(WorkerEvent::Headers {
            status,
            headers,
        })))
    }

    /// A worker can retire on its own idle timeout while still parked in the
    /// idle pool, so this must be checked before handing one out.
    pub fn worker_has_exited(&self) -> bool {
        self.worker_gone.load(Ordering::Relaxed)
    }
}

fn unexpected_marker() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "expected a response frame, got the trailing worker-done marker instead",
    )
}

/// Watches for the worker's exit: EOF is expected, and any byte arriving is
/// itself a fatal protocol violation. Either way it wakes both kinds of
/// waiter - `mark_peer_dead` alone reaches only the futex ones, since it
/// knows nothing outside shared memory.
fn spawn_liveness_watcher(
    liveness_fd: OwnedFd,
    mapped: Arc<shm::MappedChannel>,
    worker_gone: Arc<AtomicBool>,
    notify: shm::NotifyEfds,
) -> std::io::Result<()> {
    let std_stream = std::os::unix::net::UnixStream::from(liveness_fd);
    std_stream.set_nonblocking(true)?;
    let mut stream = TokioUnixStream::from_std(std_stream)?;
    tokio::spawn(async move {
        let mut buf = [0u8; 1];
        let _ = stream.read(&mut buf).await;
        mapped.channel().mark_peer_dead();
        // Unconditional: this fires once in a worker's life, so there is no
        // hot-path cost to save by checking for a parked waiter first.
        shm::eventfd_notify(notify.req_space.as_raw_fd());
        shm::eventfd_notify(notify.resp_data.as_raw_fd());
        worker_gone.store(true, Ordering::Relaxed);
    });
    Ok(())
}

#[cfg(test)]
#[path = "worker_channel_tests.rs"]
mod tests;
