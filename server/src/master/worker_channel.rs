//! Master-side bridge from the ring protocol onto async. One tokio task per
//! worker owns the ring calls and parks on an eventfd, so no tokio worker
//! thread ever blocks on a futex.

use super::pool_manager;
use crate::ipc::control::WorkerReadyFds;
use crate::ipc::data::{self, PhpRequest, ResponseFrame};
use crate::ipc::shm;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncReadExt;
use tokio::io::unix::AsyncFd;
use tokio::net::UnixStream as TokioUnixStream;
use tokio::sync::mpsc;

/// Bounds one run of `Headers` frames. The run length is worker-controlled,
/// so leaving it unbounded is an amplification risk against a shared process,
/// not merely a memory one.
const MAX_PENDING_HEADERS_BYTES: usize = 16 * 1024 * 1024;

/// One pooled worker's data channel, reused for that worker's whole life.
pub struct WorkerChannel {
    /// Unbounded, so sending never blocks and `write_request` can stay sync.
    /// Carries the request rather than pre-encoded bytes, letting `io_task`
    /// encode into a buffer it reuses; the `Arc` makes a dispatch retry a
    /// refcount bump instead of a second encode.
    request_tx: mpsc::UnboundedSender<Arc<PhpRequest<'static>>>,
    /// Bounded, so a slow HTTP client backpressures the IO task instead of
    /// letting it buffer a whole unread response.
    response_rx: mpsc::Receiver<std::io::Result<Option<ResponseFrame<'static>>>>,
    /// A plain flag rather than a `watch`: nothing awaits it, and it is read
    /// on every pop from the idle pool, where an atomic load beats
    /// `watch::Receiver::borrow` taking an internal read lock.
    worker_gone: Arc<AtomicBool>,
    mapped: Arc<shm::MappedChannel>,
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
    /// Spawns the IO task and liveness watcher, both torn down on drop.
    pub fn new(fds: WorkerReadyFds, pid: u32) -> std::io::Result<Self> {
        let WorkerReadyFds {
            channel: channel_fd,
            liveness: liveness_fd,
            notify,
        } = fds;
        let mapped = Arc::new(shm::map_existing_channel(channel_fd)?);

        // Its own dup'd fds, so it can never notify through a number the IO
        // task has closed and the OS has reused.
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

        let (request_tx, request_rx) = mpsc::unbounded_channel::<Arc<PhpRequest<'static>>>();
        let (response_tx, response_rx) = mpsc::channel(8);
        {
            let mapped = Arc::clone(&mapped);
            tokio::spawn(io_task(
                mapped,
                pid,
                request_rx,
                response_tx,
                req_space_efd,
                resp_data_efd,
            ));
        }

        Ok(WorkerChannel {
            request_tx,
            response_rx,
            worker_gone,
            mapped,
        })
    }

    /// Never blocks, so it is safe to call from async code without a timeout
    /// of its own. The encode happens in `io_task`.
    pub fn write_request(&self, req: Arc<PhpRequest<'static>>) -> std::io::Result<()> {
        self.request_tx.send(req).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "worker IO task has exited")
        })
    }

    /// The next frame. Callers loop until `End`.
    pub async fn read_response_frame(&mut self) -> std::io::Result<ResponseFrame<'static>> {
        match self.response_rx.recv().await {
            Some(Ok(Some(frame))) => Ok(frame),
            Some(Ok(None)) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected a ResponseFrame, got the trailing worker-done marker instead",
            )),
            Some(Err(e)) => Err(e),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "worker IO task has exited",
            )),
        }
    }

    /// A real frame arriving here means the wire desynced, which is a hard
    /// error rather than something to swallow.
    pub async fn read_worker_done(&mut self) -> std::io::Result<()> {
        match self.response_rx.recv().await {
            Some(Ok(None)) => Ok(()),
            Some(Ok(Some(_))) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected the worker-done marker, got a framed ResponseFrame instead",
            )),
            Some(Err(e)) => Err(e),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "worker IO task has exited",
            )),
        }
    }

    /// A worker can retire on its own idle timeout while still parked in the
    /// idle pool, so this must be checked before handing one out.
    pub fn worker_has_exited(&self) -> bool {
        self.worker_gone.load(Ordering::Relaxed)
    }
}

/// Ends once `request_tx` drops or the ring reports the peer gone, both
/// fatal for this connection.
async fn io_task(
    mapped: Arc<shm::MappedChannel>,
    pid: u32,
    mut request_rx: mpsc::UnboundedReceiver<Arc<PhpRequest<'static>>>,
    response_tx: mpsc::Sender<std::io::Result<Option<ResponseFrame<'static>>>>,
    req_space_efd: AsyncFd<OwnedFd>,
    resp_data_efd: AsyncFd<OwnedFd>,
) {
    let channel = mapped.channel();
    let mut scratch = Vec::new();
    // Both reused for the worker's whole life, so a steady-state request
    // neither encodes nor decodes with an allocation.
    let mut encode_scratch = Vec::new();
    'commands: loop {
        let Some(req) = request_rx.recv().await else {
            return;
        };

        let encoded: &[u8] = match data::encode_request(&mut encode_scratch, &req) {
            Ok(bytes) => bytes,
            Err(e) => {
                let _ = response_tx.send(Err(e)).await;
                return;
            }
        };
        if let Err(e) = data::write_request_to_ring(
            &channel.request,
            &channel.peer_death,
            encoded,
            &req_space_efd,
        )
        .await
        {
            let _ = response_tx.send(Err(e)).await;
            return;
        }
        // Accumulates a run of Headers frames so nothing downstream sees the
        // split. None means no run in progress.
        let mut pending_headers: Option<(u16, data::HeaderBlob)> = None;
        let mut pending_headers_bytes: usize = 0;
        loop {
            match data::read_response_frame_from_ring(&mapped, &mut scratch, &resp_data_efd).await {
                Ok(Some(ResponseFrame::Headers {
                    status,
                    headers,
                    more,
                })) => {
                    pending_headers_bytes += scratch.len();
                    if pending_headers_bytes > MAX_PENDING_HEADERS_BYTES {
                        // Abandoning the ring is not enough: the worker would
                        // park forever in its untimed wait_for_space, whose
                        // only other escape is real process death.
                        pool_manager::sigkill(
                            pid,
                            "worker sent an oversized run of Headers frames",
                        );
                        let _ = response_tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "worker sent an oversized run of Headers frames",
                            )))
                            .await;
                        return;
                    }
                    match &mut pending_headers {
                        Some((_, acc)) => acc.append(&headers),
                        None => pending_headers = Some((status, headers)),
                    }
                    // Forwarding on the run's last frame, rather than waiting
                    // for the next one to imply the run ended, keeps a script
                    // whose header() calls and first output are not
                    // back-to-back from paying a round-trip of TTFB.
                    if !more {
                        let (status, headers) =
                            pending_headers.take().expect("just inserted above");
                        if response_tx
                            .send(Ok(Some(ResponseFrame::Headers {
                                status,
                                headers,
                                more: false,
                            })))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Ok(None) => {
                    // A worker that fails mid-run still writes this marker
                    // straight past End; without the flush its collected
                    // headers would be silently dropped.
                    if let Some((status, headers)) = pending_headers.take() {
                        if response_tx
                            .send(Ok(Some(ResponseFrame::Headers {
                                status,
                                headers,
                                more: false,
                            })))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if response_tx.send(Ok(None)).await.is_err() {
                        return;
                    }
                    continue 'commands;
                }
                Ok(Some(frame)) => {
                    if let Some((status, headers)) = pending_headers.take() {
                        if response_tx
                            .send(Ok(Some(ResponseFrame::Headers {
                                status,
                                headers,
                                more: false,
                            })))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if response_tx.send(Ok(Some(frame))).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = response_tx.send(Err(e)).await;
                    return;
                }
            }
        }
    }
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
