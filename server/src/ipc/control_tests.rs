use super::*;
use std::io::Write;

/// A blocking read must actually time out rather than hang: the fork-server
/// loop depends on that wakeup to reap zombies with no `SPAWN` arriving.
#[test]
fn set_recv_timeout_makes_a_blocking_read_time_out() {
    let (mut a, _b) = StdUnixStream::pair().expect("socketpair failed");
    set_recv_timeout(a.as_raw_fd(), Duration::from_millis(50)).expect("setsockopt failed");

    let start = std::time::Instant::now();
    let mut buf = [0u8; 8];
    let err = a
        .read(&mut buf)
        .expect_err("read on an empty, otherwise-idle socket must time out");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "read took {:?}, way longer than the 50ms timeout - SO_RCVTIMEO apparently didn't take effect",
        start.elapsed()
    );
}

/// A reply with no fds at all is the simplest possible desync between master
/// and the prototype - must be rejected, not panic on unwrapping the pid
/// bytes or the "READY" suffix.
#[tokio::test]
async fn request_worker_rejects_a_reply_that_is_not_ready() {
    let (master_end, prototype_end) = UnixSeqpacket::pair().expect("socketpair failed");

    let responder = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        prototype_end
            .recv(&mut buf)
            .await
            .expect("SPAWN never arrived");
        prototype_end
            .send(b"garbage, not a worker-ready reply")
            .await
            .expect("send failed");
    });

    let err = match request_worker(&master_end).await {
        Ok(_) => panic!("a malformed reply must not be accepted as WORKER_READY"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("malformed WORKER_READY"),
        "got: {err}"
    );
    responder.await.unwrap();
}

/// The reply carries the right "READY" marker but the wrong number of fds -
/// a desync in the fixed 4-fd order `send_worker_ready` packs and
/// `request_worker` unpacks would otherwise misattribute one fd for another.
#[tokio::test]
async fn request_worker_rejects_the_wrong_number_of_fds() {
    let (master_end, prototype_end) = UnixSeqpacket::pair().expect("socketpair failed");

    let responder = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        prototype_end
            .recv(&mut buf)
            .await
            .expect("SPAWN never arrived");

        let mut payload = 4242u32.to_le_bytes().to_vec();
        payload.extend_from_slice(b"READY");

        // 2 fds where exactly 4 are expected; /dev/null stands in for real
        // channel/eventfd/link fds since only the count matters here.
        let dummy_a = std::fs::File::open("/dev/null").expect("open /dev/null failed");
        let dummy_b = std::fs::File::open("/dev/null").expect("open /dev/null failed");
        let raw_fds = [dummy_a.as_raw_fd(), dummy_b.as_raw_fd()];
        let cmsg = [ControlMessage::ScmRights(&raw_fds)];
        let iov = [IoSlice::new(&payload)];
        sendmsg::<()>(
            prototype_end.as_raw_fd(),
            &iov,
            &cmsg,
            MsgFlags::empty(),
            None,
        )
        .expect("sendmsg failed");
    });

    let err = match request_worker(&master_end).await {
        Ok(_) => panic!("the wrong fd count must not be silently truncated or padded"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("expected exactly 4 fds"),
        "got: {err}"
    );
    responder.await.unwrap();
}

/// `Ok(None)` on EOF, distinct from an actual read error: the fork-server
/// loop treats the two very differently (exit cleanly vs. log and retry).
#[test]
fn recv_command_returns_none_on_eof() {
    let (mut prototype_side, master_side) = StdUnixStream::pair().expect("socketpair failed");
    drop(master_side);

    let result = recv_command(&mut prototype_side).expect("EOF must not be an error");
    assert_eq!(result, None);
}

/// The happy path for the prototype's send/recv pair, exercised without any
/// fds involved - `send_worker_ready`'s payload framing is otherwise only
/// covered indirectly through `request_worker`'s own tests.
#[test]
fn recv_command_returns_the_bytes_actually_sent() {
    let (mut a, mut b) = StdUnixStream::pair().expect("socketpair failed");
    a.write_all(SPAWN).expect("write failed");

    let result = recv_command(&mut b).expect("read failed");
    assert_eq!(result.as_deref(), Some(SPAWN));
}
