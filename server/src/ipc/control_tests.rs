use super::*;

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
