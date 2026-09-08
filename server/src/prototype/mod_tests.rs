use super::*;
use std::os::fd::AsFd;

/// The startup gate must recognise the real thing, not merely reject
/// everything.
#[test]
fn is_seqpacket_socket_accepts_a_real_seqpacket_pair() {
    let (a, _b) = socketpair(AddressFamily::Unix, SockType::SeqPacket, None, SockFlag::empty())
        .expect("socketpair failed");
    assert!(is_seqpacket_socket(a.as_fd()));
}

/// What actually rejects a manual invocation: a plain `SOCK_STREAM`, or in
/// practice nothing open on that fd at all.
#[test]
fn is_seqpacket_socket_rejects_a_stream_socket() {
    let (a, _b) = socketpair(AddressFamily::Unix, SockType::Stream, None, SockFlag::empty())
        .expect("socketpair failed");
    assert!(!is_seqpacket_socket(a.as_fd()));
}
