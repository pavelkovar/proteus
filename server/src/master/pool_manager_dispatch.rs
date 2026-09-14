//! Request dispatch and response streaming: acquire capacity, send the
//! request, then own the worker and permit for the response's life.
//!
//! A child module rather than a sibling, so these `impl PoolManager` methods
//! still reach private fields.

use super::{PoolManager, PooledWorker, TempBodyFile, sigkill};
use crate::ipc::data::{HeaderBlob, PhpRequest};
use crate::logging;
use crate::master::worker_channel::WorkerEvent;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;

/// Owns a worker between check-out and the hand-off to its completion task,
/// so cancellation in between releases it instead of losing its `workers`
/// entry and pool slot for good.
struct CheckedOutWorker {
    pool: Arc<PoolManager>,
    worker: Option<PooledWorker>,
}

impl CheckedOutWorker {
    fn new(pool: Arc<PoolManager>, worker: PooledWorker) -> Self {
        CheckedOutWorker {
            pool,
            worker: Some(worker),
        }
    }

    fn get_mut(&mut self) -> &mut PooledWorker {
        self.worker.as_mut().expect("held until taken")
    }

    fn take(&mut self) -> PooledWorker {
        self.worker.take().expect("taken at most once")
    }
}

impl Drop for CheckedOutWorker {
    fn drop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return; // handed off, nothing to clean up
        };
        let pid = worker.pid;
        logging::debug!(
            r#type = "controller",
            pid,
            "request abandoned while a worker was checked out, releasing it"
        );
        self.pool.note_worker_abandoned();
        // Before the drop, whose channel close is what retires the worker.
        self.pool.release_abandoned_worker(pid);
        drop(worker);
    }
}

/// Headers known; the body may still be arriving.
pub type BodyStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = std::io::Result<Bytes>> + Send + Sync>>;

/// Past this a worker is streaming rather than answering, so the rest of its
/// output belongs on the stream instead of in one buffered reply. One body
/// frame, which is also the overshoot of a budget checked before each read:
/// the sweep holds at most two frames however fast the worker refills.
const MAX_DRAINED_PREFIX_BYTES: usize = crate::worker::COALESCE_FLUSH_THRESHOLD;

/// One buffer for the whole reply, without copying the common case of a
/// response the worker wrote in a single frame.
fn join(mut chunks: Vec<Bytes>) -> Bytes {
    if chunks.len() == 1 {
        return chunks.pop().expect("just checked");
    }
    let mut body = Vec::with_capacity(chunks.iter().map(Bytes::len).sum());
    for chunk in &chunks {
        body.extend_from_slice(chunk);
    }
    Bytes::from(body)
}

/// What a non-blocking sweep of the response ring found.
enum Drained {
    /// `End` reached: nothing is left for a streaming task to carry.
    Complete { body: Bytes, retiring: bool },
    /// Nothing more was ready; `prefix` has to lead the stream.
    Pending { prefix: Vec<Bytes> },
    /// The worker broke protocol or the channel failed.
    Broken {
        prefix: Vec<Bytes>,
        error: std::io::Error,
    },
}

pub struct StreamedResponse {
    pub status: u16,
    pub headers: HeaderBlob<'static>,
    pub body: BodyStream,
    pub worker_pid: u32,
}

/// Both variants hand `permit` back so a retry can reuse it.
enum StartAttempt {
    /// Never reached a Headers frame, so a fresh retry is safe.
    WorkerUnavailable(std::io::Error, OwnedSemaphorePermit, Option<TempBodyFile>),
    /// The request will not fit a ring frame. No worker can take it, so
    /// retrying only burns another one.
    RequestTooLarge(OwnedSemaphorePermit, Option<TempBodyFile>),
    /// Timed out before Headers, so retrying would only hang again.
    TimedOut(OwnedSemaphorePermit, Option<TempBodyFile>),
}

/// Worker metadata is already cleaned up by the time this is returned.
enum DispatchAttemptError {
    WorkerUnavailable(std::io::Error, OwnedSemaphorePermit, Option<TempBodyFile>),
    RequestTooLarge(OwnedSemaphorePermit),
    TimedOut(OwnedSemaphorePermit),
}

pub enum DispatchOutcome {
    Ok(StreamedResponse),

    Timeout,
    /// Never reached a worker at all.
    QueueTimeout,
    /// Too big for a request-ring frame, whichever worker took it. The head
    /// cap makes this a configuration fault rather than a client's doing.
    RequestTooLarge,
    Failed,
}

impl PoolManager {
    /// Always cleans up worker metadata on `WorkerUnavailable`.
    async fn try_dispatch_to(
        self: &Arc<Self>,
        worker: PooledWorker,
        req: &Arc<PhpRequest<'static>>,
        permit: OwnedSemaphorePermit,
        body_cleanup: Option<TempBodyFile>,
    ) -> Result<StreamedResponse, DispatchAttemptError> {
        let pid = worker.pid;
        // No lock and no lookup: the counters hang off the `Arc` already held.
        worker.meta.mark_busy(Instant::now(), self.started_at);
        match self
            .start_streaming(worker, req, permit, body_cleanup)
            .await
        {
            Ok(started) => Ok(started),
            Err(StartAttempt::TimedOut(permit, _body_cleanup)) => {
                Err(DispatchAttemptError::TimedOut(permit))
            }
            Err(StartAttempt::RequestTooLarge(permit, _body_cleanup)) => {
                Err(DispatchAttemptError::RequestTooLarge(permit))
            }
            Err(StartAttempt::WorkerUnavailable(e, permit, body_cleanup)) => {
                self.remove_worker_meta(pid);
                Err(DispatchAttemptError::WorkerUnavailable(
                    e,
                    permit,
                    body_cleanup,
                ))
            }
        }
    }

    /// The retry forces a fresh spawn rather than another `get_worker()`,
    /// which could pop a second stale worker from the same recycle burst.
    pub async fn dispatch(
        self: &Arc<Self>,
        req: &Arc<PhpRequest<'static>>,
        body_cleanup: Option<TempBodyFile>,
    ) -> DispatchOutcome {
        self.requests_total.fetch_add(1, Relaxed);

        // Rejects immediately rather than waiting out queue_timeout.
        let Some(queue_guard) =
            super::QueueDepthGuard::try_new(&self.queue_depth, self.queue_max_depth)
        else {
            self.queue_timeouts.fetch_add(1, Relaxed);
            return DispatchOutcome::QueueTimeout;
        };
        let acquire_result = tokio::time::timeout(
            self.queue_timeout,
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await;
        drop(queue_guard);
        let permit = match acquire_result {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => unreachable!("semaphore never closed"),
            Err(_) => {
                self.queue_timeouts.fetch_add(1, Relaxed);
                return DispatchOutcome::QueueTimeout;
            }
        };

        let worker = match self.get_worker().await {
            Ok(w) => w,
            Err(e) => {
                return self.give_up(
                    &format!("failed to obtain a worker ({e}) - prototype may be dead"),
                    permit,
                );
            }
        };
        let pid = worker.pid;
        let (permit, body_cleanup) = match self
            .try_dispatch_to(worker, req, permit, body_cleanup)
            .await
        {
            Ok(started) => return DispatchOutcome::Ok(started),
            Err(DispatchAttemptError::RequestTooLarge(permit)) => {
                self.requests_too_large.fetch_add(1, Relaxed);
                drop(permit);
                return DispatchOutcome::RequestTooLarge;
            }
            Err(DispatchAttemptError::TimedOut(permit)) => return self.timed_out(pid, permit),
            Err(DispatchAttemptError::WorkerUnavailable(e, permit, body_cleanup)) => {
                logging::warn!(
                    r#type = "controller",
                    pid,
                    error = %e,
                    "dispatch to pooled worker failed - probably just self-retired, forcing a fresh spawn"
                );
                // Probably self-retired is not certainly: otherwise this
                // worker is now untracked, and `mark_peer_dead` cannot reach
                // one that is wedged rather than parked.
                sigkill(
                    pid,
                    "pooled worker failed its dispatch, replaced by a fresh spawn",
                );
                (permit, body_cleanup)
            }
        };

        let worker = match self.spawn_worker().await {
            Ok(w) => w,
            Err(e) => {
                return self.give_up(
                    &format!("fresh worker spawn also failed ({e}), giving up on this request"),
                    permit,
                );
            }
        };
        let pid = worker.pid;
        match self
            .try_dispatch_to(worker, req, permit, body_cleanup)
            .await
        {
            Ok(started) => DispatchOutcome::Ok(started),
            Err(DispatchAttemptError::RequestTooLarge(permit)) => {
                self.requests_too_large.fetch_add(1, Relaxed);
                drop(permit);
                DispatchOutcome::RequestTooLarge
            }
            Err(DispatchAttemptError::TimedOut(permit)) => self.timed_out(pid, permit),
            Err(DispatchAttemptError::WorkerUnavailable(e, permit, _body_cleanup)) => {
                sigkill(pid, "freshly spawned worker failed its dispatch too");
                self.give_up(
                    &format!("freshly spawned worker pid={pid} STILL failed ({e}), giving up"),
                    permit,
                )
            }
        }
    }

    /// The kill has already happened; this is the shared cleanup.
    fn timed_out(&self, pid: u32, permit: OwnedSemaphorePermit) -> DispatchOutcome {
        self.watchdog_kills.fetch_add(1, Relaxed);
        self.remove_worker_meta(pid);
        drop(permit);
        DispatchOutcome::Timeout
    }

    /// Logs, counts, releases the permit.
    fn give_up(&self, context: &str, permit: OwnedSemaphorePermit) -> DispatchOutcome {
        logging::error!(r#type = "controller", "{context}");
        self.dispatch_failed.fetch_add(1, Relaxed);
        drop(permit);
        DispatchOutcome::Failed
    }

    /// Returns once the first `Headers` frame arrives: whatever the worker
    /// has already finished goes out with it, and anything still coming is
    /// carried by a spawned task. Error paths hand `permit` back for a retry.
    async fn start_streaming(
        self: &Arc<Self>,
        worker: PooledWorker,
        req: &Arc<PhpRequest<'static>>,
        permit: OwnedSemaphorePermit,
        body_cleanup: Option<TempBodyFile>,
    ) -> Result<StreamedResponse, StartAttempt> {
        let pid = worker.pid;
        // This runs on the connection's own task and the wait below spans the
        // whole of PHP's execution, so a client disconnect drops the future
        // mid-flight. The guard makes that window cancellation-safe.
        let mut worker = CheckedOutWorker::new(Arc::clone(self), worker);
        let channel = &mut worker.get_mut().channel;
        // One window covers publishing the request and waiting for the first
        // response frame: a worker that never drains the request ring is as
        // stuck as one that never answers.
        let first = tokio::time::timeout(self.request_timeout, async {
            channel.write_request(req).await?;
            // After the frame, never before: a frame that failed to go would
            // otherwise leave this fd queued for the next request to take.
            if let Some(body) = body_cleanup.as_ref() {
                channel.send_body_fd(body.as_fd())?;
            }
            channel.read_response_frame().await
        })
        .await;
        let (status, headers) = match first {
            Ok(Ok(WorkerEvent::Headers { status, headers })) => (status, headers),
            Ok(Ok(_unexpected)) => {
                // Not cancellation: the caller accounts for this one.
                drop(worker.take());
                return Err(StartAttempt::WorkerUnavailable(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "expected a Headers frame first",
                    ),
                    permit,
                    body_cleanup,
                ));
            }
            Ok(Err(e)) => {
                if e.kind() == std::io::ErrorKind::InvalidInput {
                    // Nothing was written, so this worker is still good.
                    let mut w = worker.take();
                    w.channel.shrink_scratch();
                    w.channel.park_notify();
                    w.meta.mark_idle(Instant::now(), self.started_at);
                    self.return_worker(w);
                    return Err(StartAttempt::RequestTooLarge(permit, body_cleanup));
                }
                drop(worker.take());
                return Err(StartAttempt::WorkerUnavailable(e, permit, body_cleanup));
            }
            // Even a worker that hung before reading the request must be
            // killed, or it runs on invisible to every later watchdog pass.
            Err(_elapsed) => {
                logging::warn!(
                    r#type = "controller",
                    pid,
                    request_timeout = ?self.request_timeout,
                    "worker exceeded request_timeout writing the request or waiting for headers"
                );
                sigkill(pid, "write_request/headers wait timed out");
                drop(worker.take());
                return Err(StartAttempt::TimedOut(permit, body_cleanup));
            }
        };

        // Past every cancellable await; a spawned task owns it from here.
        let mut worker = worker.take();
        let channel = &mut worker.channel;

        // A short response is usually already queued behind the headers, so
        // taking what is there without ever waiting lets the whole reply go
        // out in one write.
        match Self::drain_ready(channel) {
            // Must be ready on its first poll: a body that answers Pending
            // once is a body the head has already gone out without.
            Drained::Complete { body, retiring } => {
                let pool = Arc::clone(self);
                tokio::spawn(async move {
                    pool.finish_after_end(worker, permit, retiring, body_cleanup)
                        .await;
                });
                Ok(StreamedResponse {
                    status,
                    headers,
                    body: Box::pin(tokio_stream::iter([Ok(body)])),
                    worker_pid: pid,
                })
            }
            // Already-read bytes still go out, then the error ends the
            // stream; the worker cannot be pooled after this.
            Drained::Broken { prefix, error } => {
                self.kill_worker(pid, permit);
                drop(body_cleanup);
                Ok(StreamedResponse {
                    status,
                    headers,
                    body: Box::pin(tokio_stream::iter(
                        prefix
                            .into_iter()
                            .map(Ok)
                            .chain(std::iter::once(Err(error))),
                    )),
                    worker_pid: pid,
                })
            }
            Drained::Pending { prefix } => {
                // Bounded, so a slow client backpressures the send rather than
                // hoarding a whole unstreamed response in memory. Do not
                // shorten it to save the buffer: the depth is what keeps the
                // worker writing while master hands an earlier frame to hyper.
                let (body_tx, body_rx) = mpsc::channel::<std::io::Result<Bytes>>(8);

                let pool = Arc::clone(self);
                tokio::spawn(async move {
                    pool.drive_stream_to_completion(worker, permit, body_tx, body_cleanup)
                        .await;
                });

                let stream = tokio_stream::iter(prefix.into_iter().map(Ok))
                    .chain(ReceiverStream::new(body_rx));
                Ok(StreamedResponse {
                    status,
                    headers,
                    body: Box::pin(stream),
                    worker_pid: pid,
                })
            }
        }
    }

    /// Takes whatever the worker has already written, without ever waiting:
    /// a response that is not finished yet must not be delayed for one that
    /// might be. Bounded because each read frees ring space the worker can
    /// refill, so an unbounded loop would follow a fast producer instead of
    /// returning.
    fn drain_ready(channel: &mut crate::master::worker_channel::WorkerChannel) -> Drained {
        let mut prefix: Vec<Bytes> = Vec::new();
        let mut drained = 0;
        loop {
            if drained >= MAX_DRAINED_PREFIX_BYTES {
                return Drained::Pending { prefix };
            }
            match channel.try_read_response_frame() {
                Some(Ok(WorkerEvent::Body(chunk))) => {
                    drained += chunk.len();
                    prefix.push(chunk);
                }
                Some(Ok(WorkerEvent::End { retiring })) => {
                    return Drained::Complete {
                        body: join(prefix),
                        retiring,
                    };
                }
                Some(Ok(WorkerEvent::Headers { .. })) => {
                    return Drained::Broken {
                        prefix,
                        error: std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "worker sent a second Headers frame, protocol violation",
                        ),
                    };
                }
                Some(Err(error)) => return Drained::Broken { prefix, error },
                None => return Drained::Pending { prefix },
            }
        }
    }

    /// Owns the worker and permit for the rest of the response, returning it
    /// to idle only once the done marker arrives - which after an early
    /// `fastcgi_finish_request()` can be long after `End`.
    ///
    /// The watchdog resets per frame: slow but steady is not hung.
    async fn drive_stream_to_completion(
        self: Arc<Self>,
        mut worker: PooledWorker,
        permit: OwnedSemaphorePermit,
        body_tx: mpsc::Sender<std::io::Result<Bytes>>,
        _body_cleanup: Option<TempBodyFile>,
    ) {
        let pid = worker.pid;
        let retiring = loop {
            match tokio::time::timeout(self.request_timeout, worker.channel.read_response_frame())
                .await
            {
                Ok(Ok(WorkerEvent::Body(chunk))) => {
                    if body_tx.send(Ok(chunk)).await.is_err() {
                        // Client gone: the worker's state can no longer be
                        // trusted enough to pool it, but this is not its fault.
                        logging::debug!(
                            r#type = "controller",
                            pid,
                            "body receiver dropped (client gone), killing worker"
                        );
                        self.kill_worker(pid, permit);
                        return;
                    }
                }
                Ok(Ok(WorkerEvent::End { retiring })) => break retiring,
                Ok(Ok(WorkerEvent::Headers { .. })) => {
                    let _ = body_tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unexpected second Headers frame",
                        )))
                        .await;
                    logging::error!(
                        r#type = "controller",
                        pid,
                        "worker sent a second Headers frame, protocol violation"
                    );
                    self.watchdog_kills.fetch_add(1, Relaxed);
                    self.kill_worker(pid, permit);
                    return;
                }
                Ok(Err(e)) => {
                    let _ = body_tx
                        .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                        .await;
                    self.read_failed(
                        pid,
                        permit,
                        &format!("response stream read failed ({e}), not returning it to the pool"),
                    );
                    return;
                }
                Err(_elapsed) => {
                    let _ = body_tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "worker exceeded request_timeout mid-response",
                        )))
                        .await;
                    logging::warn!(
                        r#type = "controller",
                        pid,
                        request_timeout = ?self.request_timeout,
                        "worker exceeded request_timeout mid-response, sending SIGKILL"
                    );
                    self.watchdog_kills.fetch_add(1, Relaxed);
                    self.kill_worker(pid, permit);
                    return;
                }
            }
        };
        drop(body_tx); // ends the body stream cleanly (EOF) for the HTTP client
        self.finish_after_end(worker, permit, retiring, None).await;
    }

    /// Everything after `End`: the worker may still be running past
    /// `fastcgi_finish_request()`, so it is not poolable until its done
    /// marker arrives.
    async fn finish_after_end(
        self: Arc<Self>,
        mut worker: PooledWorker,
        permit: OwnedSemaphorePermit,
        retiring: bool,
        _body_cleanup: Option<TempBodyFile>,
    ) {
        let pid = worker.pid;
        if retiring {
            // The worker exits right after this, so there is no done marker
            // coming and nothing to return to the pool.
            logging::debug!(
                r#type = "controller",
                pid,
                "worker self-retired after limits.requests"
            );
            self.recycled_request_limit.fetch_add(1, Relaxed);
            self.remove_worker_meta(pid);
            drop(permit);
            return;
        }

        match tokio::time::timeout(self.request_timeout, worker.channel.read_worker_done()).await {
            Ok(Ok(())) => {
                let now = Instant::now();
                // Past the done marker nothing borrows the scratch, and the
                // eventfds can leave this runtime's reactor.
                worker.channel.shrink_scratch();
                worker.channel.park_notify();
                worker.meta.mark_idle(now, self.started_at);
                self.return_worker(worker);
                drop(permit);
            }
            Ok(Err(e)) => {
                self.read_failed(
                    pid,
                    permit,
                    &format!(
                        "trailing worker-done read failed ({e}), not returning it to the pool"
                    ),
                );
            }
            Err(_elapsed) => {
                logging::warn!(
                    r#type = "controller",
                    pid,
                    request_timeout = ?self.request_timeout,
                    "worker exceeded request_timeout finishing work after fastcgi_finish_request(), sending SIGKILL"
                );
                self.watchdog_kills.fetch_add(1, Relaxed);
                self.kill_worker(pid, permit);
            }
        }
    }

    /// Bumps no counter itself: a watchdog timeout and a disconnected client
    /// both end in a kill, but only one is the worker's fault.
    fn kill_worker(&self, pid: u32, permit: OwnedSemaphorePermit) {
        sigkill(pid, "drive_stream_to_completion");
        self.remove_worker_meta(pid);
        drop(permit);
    }

    /// A read failure rather than a timeout, but still a kill: dropping the
    /// channel gets a *parked* worker to exit on its own, while one still
    /// inside `execute_file` would run on unowned and untracked.
    fn read_failed(&self, pid: u32, permit: OwnedSemaphorePermit, context: &str) {
        logging::warn!(r#type = "controller", pid, "{context}");
        self.dispatch_failed.fetch_add(1, Relaxed);
        sigkill(pid, "response stream failed, worker abandoned");
        self.remove_worker_meta(pid);
        drop(permit);
    }
}

#[cfg(test)]
#[path = "pool_manager_dispatch_tests.rs"]
mod tests;
