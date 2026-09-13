use super::*;

/// These tests never spill a body, so the far end can go straight away.
fn unused_body_socket() -> std::os::fd::OwnedFd {
    let (a, _b) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::SeqPacket,
        None,
        nix::sys::socket::SockFlag::empty(),
    )
    .expect("socketpair");
    a
}
use crate::ipc::data;
use bytes::Bytes;
use std::os::fd::AsRawFd;
use std::time::Duration;

/// A real `WorkerChannel` over a real memfd channel, with the handles a test
/// needs to act as the worker would - writing into the response ring
/// directly, without a second process.
struct Harness {
    worker_side: shm::MappedChannel,
    req_space_efd_raw: std::os::fd::RawFd,
    resp_data_efd_raw: std::os::fd::RawFd,
    channel: WorkerChannel,
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
    let req_space_efd_raw = req_space_efd_owned.as_raw_fd();
    let resp_data_efd_raw = resp_data_efd_owned.as_raw_fd();

    Harness {
        worker_side,
        req_space_efd_raw,
        resp_data_efd_raw,
        channel: WorkerChannel::for_test(
            pid,
            Arc::new(master_side),
            shm::NotifyEfds {
                req_space: req_space_efd_owned,
                resp_data: resp_data_efd_owned,
            },
            unused_body_socket(),
        ),
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
async fn write_request_publishes_the_request_for_the_worker_to_read() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    h.channel.write_request(&dummy_request()).await.unwrap();

    let channel = h.worker_side.channel();
    let mut scratch = Vec::new();
    let command = data::read_command_from_ring(
        &channel.request,
        &channel.peer_death,
        &mut scratch,
        h.req_space_efd_raw,
        Some(std::time::Instant::now() + Duration::from_secs(5)),
    )
    .unwrap()
    .unwrap();
    match command {
        data::WorkerCommand::Request(req) => assert_eq!(req.script_name, "/x.php"),
        // What an expired deadline yields, so this is also "nothing arrived".
        data::WorkerCommand::Retire => panic!("no request reached the ring"),
    }
}

#[tokio::test]
async fn try_read_response_frame_reports_nothing_ready_rather_than_end_of_stream() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    assert!(
        h.channel.try_read_response_frame().is_none(),
        "an empty ring is not end of stream"
    );

    let channel = h.worker_side.channel();
    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Test", "1");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: false,
        },
        h.resp_data_efd_raw,
    );

    let got = h
        .channel
        .try_read_response_frame()
        .expect("the frame was published")
        .unwrap();
    assert!(matches!(got, WorkerEvent::Headers { status: 200, .. }));
    assert!(h.channel.try_read_response_frame().is_none());
}

/// The accumulator outlives a single call, so a run that straddles two
/// non-blocking reads must still arrive downstream as one frame.
#[tokio::test]
async fn a_headers_run_split_across_non_blocking_reads_is_still_joined() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();

    let mut first = data::HeaderBlob::default();
    first.push("X-One", "1");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&first).into(),
            more: true,
        },
        h.resp_data_efd_raw,
    );
    assert!(
        h.channel.try_read_response_frame().is_none(),
        "an unfinished run must not be handed out"
    );

    let mut second = data::HeaderBlob::default();
    second.push("X-Two", "2");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&second).into(),
            more: false,
        },
        h.resp_data_efd_raw,
    );

    let got = h
        .channel
        .try_read_response_frame()
        .expect("the run finished")
        .unwrap();
    let WorkerEvent::Headers { headers, .. } = got else {
        panic!("expected the joined Headers frame");
    };
    let names: Vec<&str> = headers.iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["X-One", "X-Two"]);
}

/// The cap bounds one request's run, not a worker's whole life: without the
/// reset, a worker serving enough requests is eventually killed for headers
/// it never held at once. Reads the counter directly because provoking this
/// end to end costs two requests of 16 MiB.
#[tokio::test]
async fn the_headers_run_budget_is_reset_for_each_request() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();

    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Test", "1");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: false,
        },
        h.resp_data_efd_raw,
    );
    tokio::time::timeout(Duration::from_secs(5), h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(h.channel.pending_headers_bytes > 0);
    h.channel.deferred = Some(Some(WorkerEvent::Body(Bytes::from_static(b"stale"))));

    h.channel.write_request(&dummy_request()).await.unwrap();
    assert_eq!(h.channel.pending_headers_bytes, 0);
    assert!(
        h.channel.deferred.is_none(),
        "a new request must not inherit the previous response's frame"
    );
}

/// The non-blocking path is the one `drain_ready` uses right after the
/// headers, so it is exactly where a flush-displaced frame would go missing.
#[tokio::test]
async fn a_non_blocking_read_hands_out_the_frame_a_flush_displaced() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();

    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-One", "1");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: true,
        },
        h.resp_data_efd_raw,
    );
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Body(b"hi"),
        h.resp_data_efd_raw,
    );

    let got = tokio::time::timeout(Duration::from_secs(5), h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(got, WorkerEvent::Headers { .. }));

    let got = h
        .channel
        .try_read_response_frame()
        .expect("the displaced body frame must still be there")
        .unwrap();
    assert!(matches!(got, WorkerEvent::Body(ref b) if b.as_ref() == b"hi"));
}

/// A run can also be ended by the next frame arriving rather than by a
/// `more: false` one, and the joined headers must still come out ahead of it.
#[tokio::test]
async fn a_headers_run_ended_by_a_body_frame_keeps_the_workers_order() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);
    let channel = h.worker_side.channel();

    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-One", "1");
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: true,
        },
        h.resp_data_efd_raw,
    );
    write_frame(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Body(b"hi"),
        h.resp_data_efd_raw,
    );

    let deadline = Duration::from_secs(5);
    let got = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    let WorkerEvent::Headers { headers, .. } = got else {
        panic!("the pending run must be flushed before the body frame");
    };
    let names: Vec<&str> = headers.iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["X-One"]);

    let got = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .expect("the body frame the flush displaced must still be handed out")
        .unwrap();
    assert!(matches!(got, WorkerEvent::Body(ref b) if b.as_ref() == b"hi"));
}

#[tokio::test]
async fn a_normal_small_headers_run_is_reassembled_end_to_end() {
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
    let got_headers = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        got_headers,
        WorkerEvent::Headers { status: 200, .. }
    ));
    let got_body = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(got_body, WorkerEvent::Body(ref b) if b.as_ref() == b"hi"));
    let got_end = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(got_end, WorkerEvent::End { retiring: false }));
    tokio::time::timeout(deadline, h.channel.read_worker_done())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_complete_headers_frame_is_forwarded_before_the_worker_sends_anything_else() {
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
    let got_headers = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .expect("Headers must be forwarded on its own, without waiting for a Body/End frame that was never sent")
        .unwrap();
    assert!(matches!(
        got_headers,
        WorkerEvent::Headers { status: 200, .. }
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
    let got_body = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(got_body, WorkerEvent::Body(ref b) if b.as_ref() == b"hi"));
    let got_end = tokio::time::timeout(deadline, h.channel.read_response_frame())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(got_end, WorkerEvent::End { retiring: false }));
    tokio::time::timeout(deadline, h.channel.read_worker_done())
        .await
        .unwrap()
        .unwrap();
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
        body: unused_body_socket(),
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
async fn a_worker_that_never_stops_sending_headers_frames_is_rejected() {
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
    let mut channel = h.channel;

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
                return; // peer already gone once the reader bails - fine
            }
        }
    });

    let deadline = Duration::from_secs(20);
    loop {
        match tokio::time::timeout(deadline, channel.read_response_frame())
            .await
            .expect("should not hang")
        {
            Err(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                break;
            }
            Ok(WorkerEvent::Headers { .. }) => continue, // still within the (temporary) run
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
        body: unused_body_socket(),
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

/// Registration is all or nothing: a regular file cannot join an epoll set, so
/// the second half fails where the first succeeded - and the eventfds are the
/// only way this channel can ever notify its peer.
#[tokio::test]
async fn a_failed_registration_leaves_the_channel_parked_with_both_fds() {
    use std::os::fd::OwnedFd;
    let req_space = shm::create_notify_eventfd().unwrap();
    let resp_data: OwnedFd = std::fs::File::open(std::env::current_exe().unwrap())
        .unwrap()
        .into();
    let mut notify = Notify::Parked(shm::NotifyEfds {
        req_space,
        resp_data,
    });

    assert!(
        notify.register().is_err(),
        "a regular file was accepted into the epoll set"
    );
    assert!(
        matches!(notify, Notify::Parked(_)),
        "left mid-transition instead of rolled back"
    );
    assert!(notify.registered().is_none());

    // Both fds survived, so this fails the same way rather than on a closed fd.
    assert!(notify.register().is_err());
    assert!(matches!(notify, Notify::Parked(_)));
}

/// Parking is called on the way back to the idle pool, and a channel that was
/// never registered is already parked. Doing it anyway must leave the channel
/// usable rather than consuming the eventfds it was meant to keep.
#[tokio::test]
async fn parking_a_channel_that_was_never_registered_keeps_it_usable() {
    let mut h = spawn_harness(NO_REAL_WORKER_PID);

    // Nothing has been dispatched to it, so this is the already-parked case.
    h.channel.park_notify();

    h.channel
        .write_request(&dummy_request())
        .await
        .expect("a parked channel must still be able to register and write");
}
