use super::*;
use std::os::fd::AsFd;

/// The startup gate must recognise the real thing, not merely reject
/// everything.
#[test]
fn is_seqpacket_socket_accepts_a_real_seqpacket_pair() {
    let (a, _b) = socketpair(
        AddressFamily::Unix,
        SockType::SeqPacket,
        None,
        SockFlag::empty(),
    )
    .expect("socketpair failed");
    assert!(is_seqpacket_socket(a.as_fd()));
}

/// What actually rejects a manual invocation: a plain `SOCK_STREAM`, or in
/// practice nothing open on that fd at all.
#[test]
fn is_seqpacket_socket_rejects_a_stream_socket() {
    let (a, _b) = socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    )
    .expect("socketpair failed");
    assert!(!is_seqpacket_socket(a.as_fd()));
}

fn has_cloexec(fd: std::os::fd::BorrowedFd<'_>) -> bool {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    FdFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFD).unwrap()).contains(FdFlag::FD_CLOEXEC)
}

/// Master reads EOF as the worker's exit; an exec()'d process must not hold it open.
#[test]
fn neither_end_of_the_worker_link_survives_an_exec() {
    let (master_side, worker_side) = worker_link_pair().unwrap();
    assert!(has_cloexec(master_side.as_fd()), "master's end");
    assert!(has_cloexec(worker_side.as_fd()), "the worker's end");
}

fn sleeper() -> std::process::Child {
    std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .expect("failed to spawn sleep")
}

fn pid_of(child: &std::process::Child) -> nix::unistd::Pid {
    nix::unistd::Pid::from_raw(child.id() as i32)
}

fn ended(child: &mut std::process::Child) -> bool {
    for _ in 0..50 {
        if child.try_wait().unwrap().is_some() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    false
}

#[test]
fn a_kill_reaches_a_live_child_and_nothing_else() {
    let mut ours = sleeper();
    let mut stranger = sleeper();
    let mut children = Children::default();
    children.forked(pid_of(&ours));

    children.kill(ours.id());
    children.kill(stranger.id());

    assert!(
        ended(&mut ours),
        "the prototype's own worker survived its kill"
    );
    assert!(
        stranger.try_wait().unwrap().is_none(),
        "a process that was never this prototype's worker was signalled"
    );
    let _ = stranger.kill();
    let _ = stranger.wait();
}

#[test]
fn a_reaped_child_is_never_signalled_again() {
    let mut reused = sleeper();
    let mut children = Children::default();
    children.forked(pid_of(&reused));
    // As if the kernel had reused the reaped worker's pid.
    children.reaped(pid_of(&reused));

    children.kill(reused.id());

    assert!(
        reused.try_wait().unwrap().is_none(),
        "a reaped pid was signalled"
    );
    let _ = reused.kill();
    let _ = reused.wait();
}
