use super::*;
use crate::ipc::data::{self, ResponseFrame};
use std::os::fd::AsRawFd;
use std::time::Duration;

/// A real `io_task` over a real memfd channel, with the handles a test needs
/// to act as the worker would - writing into the response ring directly,
/// without a second process.
struct Harness {
    worker_side: shm::MappedChannel,
    resp_data_efd_raw: std::os::fd::RawFd,
    response_rx: mpsc::Receiver<std::io::Result<Option<ResponseFrame<'static>>>>,
    request_tx: mpsc::UnboundedSender<Arc<PhpRequest<'static>>>,
}

/// Bypasses `WorkerChannel::new`, so no liveness watcher exists here and
/// `peer.is_dead()` can never become true whatever `pid` is. Most tests
/// should therefore pass an out-of-range pid, where `sigkill` fails
/// harmlessly with ESRCH rather than risking a real process.
fn spawn_harness(pid: u32) -> Harness {
    let (fd, worker_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let req_space_efd_owned = shm::create_notify_eventfd().unwrap();
    let resp_data_efd_owned = shm::create_notify_eventfd().unwrap();
    let resp_data_efd_raw = resp_data_efd_owned.as_raw_fd();
    let req_space_efd = AsyncFd::new(req_space_efd_owned).unwrap();
    let resp_data_efd = AsyncFd::new(resp_data_efd_owned).unwrap();
    let (request_tx, request_rx) = mpsc::unbounded_channel::<Arc<PhpRequest<'static>>>();
    let (response_tx, response_rx) = mpsc::channel(8);

    tokio::spawn(io_task(
        Arc::new(master_side),
        pid,
        request_rx,
        response_tx,
        req_space_efd,
        resp_data_efd,
    ));

    // Contents are irrelevant; this only moves `io_task` on to reading.
    request_tx.send(Arc::new(dummy_request())).unwrap();

    Harness {
        worker_side,
        resp_data_efd_raw,
        response_rx,
        request_tx,
    }
}

/// See `spawn_harness` for why most tests want no real process behind this.
const NO_REAL_WORKER_PID: u32 = 999_999_999;

/// Just enough to put bytes on the ring; nothing reads it back.
fn dummy_request() -> PhpRequest<'static> {
    use std::borrow::Cow;
    PhpRequest {
        script_path: Cow::Borrowed("/dev/null"),
        document_root: Cow::Borrowed("/"),
        script_name: Cow::Borrowed("/x.php"),
        path_info: Cow::Borrowed(""),
        method: Cow::Borrowed("GET"),
        uri: Cow::Borrowed("/"),
        query_string: Cow::Borrowed(""),
        content_type: Cow::Borrowed(""),
        headers: data::HeaderBlob::default(),
        client_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        body: data::RequestBody::Inline(Cow::Borrowed(&[])),
        server_name: Cow::Borrowed("localhost"),
        server_port: 80,
        server_protocol: Cow::Borrowed("HTTP/1.1"),
        https: false,
    }
}

/// Writes one frame as a real worker does, but with a throwaway scratch
/// buffer, reuse being an allocation concern rather than a behavioural one.
fn write_frame(
    response: &shm::ResponseRing,
    peer: &shm::PeerDeath,
    frame: &data::ResponseFrameRef<'_>,
    efd: std::os::fd::RawFd,
) {
    let mut scratch = Vec::new();
    data::write_response_frame_to_ring(response, peer, frame, &mut scratch, efd).unwrap();
}

#[tokio::test]
async fn io_task_reassembles_a_normal_small_headers_run_end_to_end() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();
    let peer = &channel.peer_death;
    let response = &channel.response;
    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Test", "1");
    let headers = data::ResponseFrameRef::Headers {
        status: 200,
        headers: (&header_pairs).into(),
        more: false,
    };

    write_frame(response, peer, &headers, h.resp_data_efd_raw);
    write_frame(
        response,
        peer,
        &data::ResponseFrameRef::Body(b"hi"),
        h.resp_data_efd_raw,
    );
    write_frame(
        response,
        peer,
        &data::ResponseFrameRef::End { retiring: false },
        h.resp_data_efd_raw,
    );
    data::write_worker_done_to_ring(response, peer, h.resp_data_efd_raw).unwrap();

    let deadline = Duration::from_secs(5);
    let got_headers = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        got_headers,
        Some(ResponseFrame::Headers { status: 200, .. })
    ));
    let got_body = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(got_body, Some(ResponseFrame::Body(ref b)) if b.as_ref() == b"hi"));
    let got_end = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        got_end,
        Some(ResponseFrame::End { retiring: false })
    ));
    let got_done = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got_done.is_none());

    drop(h.request_tx); // let the task end cleanly
}

#[tokio::test]
async fn io_task_forwards_a_complete_headers_frame_before_the_worker_sends_anything_else() {
    // `more: false` alone must be enough: inferring the run's end from the
    // next frame instead costs a full ring round-trip of TTFB whenever a
    // script's header() calls and first output are not back-to-back.
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();
    let peer = &channel.peer_death;
    let response = &channel.response;
    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Test", "1");
    let headers = data::ResponseFrameRef::Headers {
        status: 200,
        headers: (&header_pairs).into(),
        more: false,
    };
    write_frame(response, peer, &headers, h.resp_data_efd_raw);

    let deadline = Duration::from_millis(500);
    let got_headers = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .expect("Headers must be forwarded on its own, without waiting for a Body/End frame that was never sent")
        .unwrap()
        .unwrap();
    assert!(matches!(
        got_headers,
        Some(ResponseFrame::Headers { status: 200, .. })
    ));

    // Finish normally so the harness's task ends cleanly.
    write_frame(
        response,
        peer,
        &data::ResponseFrameRef::Body(b"hi"),
        h.resp_data_efd_raw,
    );
    write_frame(
        response,
        peer,
        &data::ResponseFrameRef::End { retiring: false },
        h.resp_data_efd_raw,
    );
    data::write_worker_done_to_ring(response, peer, h.resp_data_efd_raw).unwrap();
    let got_body = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(got_body, Some(ResponseFrame::Body(ref b)) if b.as_ref() == b"hi"));
    let got_end = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        got_end,
        Some(ResponseFrame::End { retiring: false })
    ));
    let got_done = tokio::time::timeout(deadline, h.response_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got_done.is_none());

    drop(h.request_tx);
}

/// Any byte on the liveness socket is as fatal as EOF. Goes through the real
/// `WorkerChannel::new`, since `spawn_harness` has no liveness watcher.
#[tokio::test]
async fn a_stray_byte_on_the_liveness_socket_is_treated_as_fatal_same_as_eof() {
    use nix::sys::socket::{AddressFamily, SockFlag, SockType, socketpair};

    let (channel_fd, worker_side_mapping) = shm::create_channel().unwrap();
    drop(worker_side_mapping); // only needed to create+init the memfd
    let (test_side, worker_liveness_side) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    )
    .unwrap();
    let notify = shm::NotifyEfds {
        req_space: shm::create_notify_eventfd().unwrap(),
        resp_data: shm::create_notify_eventfd().unwrap(),
    };
    let fds = WorkerReadyFds {
        channel: channel_fd,
        liveness: worker_liveness_side,
        notify,
    };
    let channel = WorkerChannel::new(fds, NO_REAL_WORKER_PID).unwrap();

    // A real worker never sends anything, so this is the protocol-violation
    // case rather than the ordinary EOF one.
    nix::unistd::write(&test_side, &[0u8]).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !channel.worker_has_exited() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a stray byte on the liveness socket must be treated as fatal, same as EOF"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Kills and reaps on drop, so a failing assertion cannot leak the child.
struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn io_task_rejects_a_worker_that_never_stops_sending_headers_frames() {
    // A `Headers` run is worker-controlled and, unlike `Body`, bounded by no
    // channel capacity. Uses a real disposable child's pid so a wrong or
    // missing kill shows up as a process still alive at the end.
    //
    // What this cannot prove: that a worker genuinely blocked mid-write is
    // released by that kill. This harness has no liveness watcher, so
    // `peer.is_dead()` never becomes true; the integration suite covers it
    // with a real forked process.
    let child = ChildGuard(
        std::process::Command::new("sleep")
            .arg("100")
            .spawn()
            .expect("failed to spawn sleep"),
    );
    let child_pid = child.0.id();

    let h = spawn_harness(child_pid);
    let resp_data_efd_raw = h.resp_data_efd_raw;
    let worker_side = h.worker_side;
    let mut response_rx = h.response_rx;

    let big_value = "v".repeat(250_000);
    // The run must never close: a `more: false` frame flushes and clears the
    // accumulator, so the cap could never trip.
    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Big", &big_value);
    let frame = data::ResponseFrameRef::Headers {
        status: 200,
        headers: (&header_pairs).into(),
        more: true,
    };
    let wire_size = postcard::to_allocvec(&frame).unwrap().len();
    assert!(
        wire_size < shm::RESPONSE_RING_CAPACITY - 4,
        "one frame must still fit the ring on its own"
    );

    // Exactly enough to cross the cap on the last frame. The writer only
    // blocks between frames, waiting for space the previous read freed, so it
    // is never mid-write when the cap-tripping frame is read.
    let frames_needed = MAX_PENDING_HEADERS_BYTES / wire_size + 1;

    let writer = std::thread::spawn(move || {
        let channel = worker_side.channel();
        let peer = &channel.peer_death;
        let response = &channel.response;
        let frame = data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: true,
        };
        let mut scratch = Vec::new();
        for _ in 0..frames_needed {
            if data::write_response_frame_to_ring(
                response,
                peer,
                &frame,
                &mut scratch,
                resp_data_efd_raw,
            )
            .is_err()
            {
                return; // peer already gone once io_task bails - fine
            }
        }
    });

    let deadline = Duration::from_secs(20);
    loop {
        match tokio::time::timeout(deadline, response_rx.recv())
            .await
            .expect("should not hang")
        {
            Some(Err(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                break;
            }
            Some(Ok(Some(ResponseFrame::Headers { .. }))) => continue, // still within the (temporary) run
            other => panic!("expected an error once the cap was crossed, got {other:?}"),
        }
    }

    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();

    // Poll for a real death rather than assume the call happened.
    let mut child = child;
    let reap_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.0.try_wait() {
            Ok(Some(_status)) => break,
            Ok(None) => {
                assert!(
                    tokio::time::Instant::now() < reap_deadline,
                    "pid={child_pid} is still alive - the cap-exceeded path never sent it SIGKILL"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
}

// --- abandoning a worker must not leak the process ---

/// A real `WorkerChannel`, with the worker's own side kept alive in the
/// returned tuple.
///
/// Holding the liveness side matters: dropping it lets the watcher see EOF
/// and set `peer_death` itself, passing the tests for the wrong reason.
fn channel_with_live_worker_side() -> (WorkerChannel, shm::MappedChannel, shm::NotifyEfds, OwnedFd)
{
    use nix::sys::socket::{AddressFamily, SockFlag, SockType, socketpair};

    let (channel_fd, worker_side) = shm::create_channel().unwrap();
    let (liveness_master_side, liveness_worker_side) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    )
    .unwrap();
    let notify = shm::NotifyEfds {
        req_space: shm::create_notify_eventfd().unwrap(),
        resp_data: shm::create_notify_eventfd().unwrap(),
    };
    // Through its own copies, as a forked worker's inherited fds would be.
    let worker_notify = notify.try_clone().unwrap();

    let fds = WorkerReadyFds {
        channel: channel_fd,
        liveness: liveness_master_side,
        notify,
    };
    let channel = WorkerChannel::new(fds, NO_REAL_WORKER_PID).unwrap();
    (channel, worker_side, worker_notify, liveness_worker_side)
}

#[tokio::test]
async fn dropping_a_worker_channel_marks_the_peer_dead() {
    let (channel, worker_side, _worker_notify, _liveness_worker_side) =
        channel_with_live_worker_side();

    assert!(
        !worker_side.channel().peer_death.is_dead(),
        "nothing should have marked the peer dead while the channel is still held"
    );

    drop(channel);

    assert!(
        worker_side.channel().peer_death.is_dead(),
        "dropping the channel is master giving up on this worker - the worker has no other way to find out"
    );
}

/// An abandoned worker parked in an untimed wait must still be released:
/// `peer_death` is otherwise set only on the worker's own exit, so it would
/// sit on the futex forever holding its PHP heap, already untracked.
///
/// `Ok(None)` rather than an error is the point - that is what `worker::run`
/// treats as master being done with it, exiting cleanly through PHP's
/// shutdown functions.
#[tokio::test]
async fn a_worker_parked_on_the_request_ring_is_released_when_master_drops_the_channel() {
    let (channel, worker_side, worker_notify, _liveness_worker_side) =
        channel_with_live_worker_side();
    let req_space_raw = worker_notify.req_space.as_raw_fd();

    // A channel, not a join handle: a regression here never returns, and
    // `join()` would hang the run rather than fail it.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let ch = worker_side.channel();
        let mut scratch = Vec::new();
        // No deadline: this is about release by peer death, not by timeout.
        let result = data::read_command_from_ring(
            &ch.request,
            &ch.peer_death,
            &mut scratch,
            req_space_raw,
            None,
        );
        let _ = done_tx.send(matches!(result, Ok(None)));
    });

    // Long enough to be genuinely parked, past the fast-path check.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        done_rx.try_recv().is_err(),
        "the worker should still be parked, with no command sent"
    );

    drop(channel);

    let released =
        tokio::task::spawn_blocking(move || done_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .expect("the parked worker was never released - it would sit on this futex forever");
    assert!(
        released,
        "an abandoned worker must see Ok(None) and exit cleanly, not an error"
    );
}

/// The same guarantee on the other ring: a worker blocked mid-response
/// because master stopped draining must be released too, not only an idle
/// one.
#[tokio::test]
async fn a_worker_blocked_writing_a_response_is_released_when_master_drops_the_channel() {
    let (channel, worker_side, worker_notify, _liveness_worker_side) =
        channel_with_live_worker_side();
    let resp_data_raw = worker_notify.resp_data.as_raw_fd();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let ch = worker_side.channel();
        // Master never reads here, so this runs out of space and parks.
        let chunk = vec![0u8; 8 * 1024];
        let frame = data::ResponseFrameRef::Body(&chunk);
        let mut scratch = Vec::new();
        loop {
            match data::write_response_frame_to_ring(
                &ch.response,
                &ch.peer_death,
                &frame,
                &mut scratch,
                resp_data_raw,
            ) {
                Ok(()) => continue,
                Err(e) => {
                    let _ = done_tx.send(e.kind());
                    return;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        done_rx.try_recv().is_err(),
        "the writer should still be blocked on a full response ring"
    );

    drop(channel);

    let kind = tokio::task::spawn_blocking(move || done_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("the blocked writer was never released - it would sit on this futex forever");
    assert_eq!(
        kind,
        std::io::ErrorKind::BrokenPipe,
        "a released writer should report the peer as gone"
    );
}
