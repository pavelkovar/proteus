//! Control channel (master <-> prototype), SOCK_SEQPACKET + SCM_RIGHTS.
//!
//! Master must close its own copy of every fd it dup2's into the child, or
//! EOF detection breaks when the prototype dies.

use crate::ipc::shm;
use crate::logging;
use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use std::io::{IoSlice, Read};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::time::Duration;
use tokio_seqpacket::UnixSeqpacket;
use tokio_seqpacket::ancillary::OwnedAncillaryMessage;

pub const SPAWN: &[u8] = b"SPAWN";

/// Followed by the pid (LE). Unanswered, so it cannot desync a `SPAWN` exchange.
const KILL: &[u8] = b"KILL";

/// Answer to a `SPAWN` that failed, followed by the errno (LE).
const SPAWN_FAILED: &[u8] = b"FAIL";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Spawn,
    Kill(u32),
    Unknown,
}

pub fn parse_command(cmd: &[u8]) -> Command {
    if cmd == SPAWN {
        return Command::Spawn;
    }
    match cmd.strip_prefix(KILL).map(<[u8; 4]>::try_from) {
        Some(Ok(pid)) => Command::Kill(u32::from_le_bytes(pid)),
        _ => Command::Unknown,
    }
}

pub fn kill_command(pid: u32) -> [u8; 8] {
    let mut msg = [0u8; 8];
    msg[..4].copy_from_slice(KILL);
    msg[4..].copy_from_slice(&pid.to_le_bytes());
    msg
}

/// The prototype is still up and in sync: respawning it would not help.
#[derive(Debug)]
pub struct SpawnRefused(pub std::io::Error);

impl std::fmt::Display for SpawnRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the prototype could not make a worker: {}", self.0)
    }
}

impl std::error::Error for SpawnRefused {}

pub fn is_spawn_refused(e: &std::io::Error) -> bool {
    e.get_ref()
        .is_some_and(|inner| inner.downcast_ref::<SpawnRefused>().is_some())
}

/// Named fields rather than a tuple, so two same-typed fds cannot be
/// transposed at a call site without a compile error.
pub struct WorkerReadyFds {
    pub channel: OwnedFd,
    pub notify: shm::NotifyEfds,
    /// Master to worker, one socket for the worker's whole life: it carries
    /// the fd of a spilled request body, which the ring cannot, and the
    /// worker's end closing is how master learns the process is gone.
    pub link: OwnedFd,
}

/// Unpacks the reply's fds in the same fixed order `send_worker_ready` packs
/// them.
pub async fn request_worker(control: &UnixSeqpacket) -> std::io::Result<(WorkerReadyFds, u32)> {
    control.send(SPAWN).await?;

    let mut buf = [0u8; 64];
    let mut ancillary_buf = [0u8; 128];
    let (msg_info, ancillary) = control
        .recv_with_ancillary(&mut buf, &mut ancillary_buf)
        .await?;

    let n = msg_info.bytes_read();
    if n == SPAWN_FAILED.len() + 4 && buf.starts_with(SPAWN_FAILED) {
        let errno = i32::from_le_bytes(buf[4..8].try_into().unwrap());
        // Anything but a real errno means the channel is out of sync.
        if (1..4096).contains(&errno) {
            return Err(std::io::Error::other(SpawnRefused(
                std::io::Error::from_raw_os_error(errno),
            )));
        }
    }
    if n < 4 || &buf[4..n] != b"READY" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("malformed WORKER_READY reply from prototype ({n} bytes)"),
        ));
    }
    let pid = u32::from_le_bytes(buf[0..4].try_into().unwrap());

    let mut fds = Vec::new();
    for message in ancillary.into_messages() {
        if let OwnedAncillaryMessage::FileDescriptors(received) = message {
            fds.extend(received);
        }
    }
    let [channel_fd, req_space_efd, resp_data_efd, link_fd] = <[OwnedFd; 4]>::try_from(fds)
        .map_err(|fds| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "expected exactly 4 fds in WORKER_READY reply, got {}",
                    fds.len()
                ),
            )
        })?;

    Ok((
        WorkerReadyFds {
            channel: channel_fd,
            notify: shm::NotifyEfds {
                req_space: req_space_efd,
                resp_data: resp_data_efd,
            },
            link: link_fd,
        },
        pid,
    ))
}

/// Prototype side, blocking. `Ok(None)` means master closed the channel.
pub fn recv_command(control: &mut StdUnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = [0u8; 32];
    match control.read(&mut buf) {
        Ok(0) => Ok(None),
        Ok(n) => Ok(Some(buf[..n].to_vec())),
        Err(e) => Err(e),
    }
}

/// One `SCM_RIGHTS` message for all of them, in the fixed order
/// `request_worker` unpacks them.
pub fn send_worker_ready(
    control: &mut StdUnixStream,
    worker_pid: i32,
    fds: WorkerReadyFds,
) -> std::io::Result<()> {
    let mut payload = (worker_pid as u32).to_le_bytes().to_vec();
    payload.extend_from_slice(b"READY");

    let raw_fds = [
        fds.channel.as_raw_fd(),
        fds.notify.req_space.as_raw_fd(),
        fds.notify.resp_data.as_raw_fd(),
        fds.link.as_raw_fd(),
    ];
    let cmsg = [ControlMessage::ScmRights(&raw_fds)];
    let iov = [IoSlice::new(&payload)];

    // The prototype takes SIGCHLD without `SA_RESTART`. Returning closes its
    // own copies of the fds; master holds duplicates.
    loop {
        match sendmsg::<()>(control.as_raw_fd(), &iov, &cmsg, MsgFlags::empty(), None) {
            Ok(_) => return Ok(()),
            Err(nix::errno::Errno::EINTR) => continue,
            Err(e) => return Err(std::io::Error::from_raw_os_error(e as i32)),
        }
    }
}

/// Prototype side.
pub fn send_spawn_failed(control: &mut StdUnixStream, err: &std::io::Error) -> std::io::Result<()> {
    use std::io::Write;
    let mut payload = SPAWN_FAILED.to_vec();
    payload.extend_from_slice(&err.raw_os_error().unwrap_or(libc::EIO).to_le_bytes());
    control.write_all(&payload)
}

/// Reaps every child that has already exited, handing each terminal status to
/// `on_exit`. Non-blocking, so zombies do not accumulate while the caller is
/// waiting on something else.
pub fn reap_exited_children(mut on_exit: impl FnMut(Pid, WaitStatus)) {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(status @ (WaitStatus::Exited(pid, _) | WaitStatus::Signaled(pid, _, _))) => {
                on_exit(pid, status)
            }
            // A stop or a continue is not an exit, and leaving the loop on one
            // would strand every zombie behind it until the next sweep.
            Ok(WaitStatus::StillAlive) => return,
            Ok(_) | Err(nix::errno::Errno::EINTR) => continue,
            Err(_) => return,
        }
    }
}

/// Prototype side: a worker's abnormal exit turns a mystery 500 into a
/// diagnosable one. `on_reaped` hears every pid that is no longer this
/// prototype's to signal.
pub fn reap_finished_workers(mut on_reaped: impl FnMut(Pid)) {
    reap_exited_children(|pid, status| {
        on_reaped(pid);
        match status {
            WaitStatus::Signaled(_, signal, _) => {
                logging::warn!(r#type = "prototype", worker_pid = %pid, ?signal, "worker killed by signal")
            }
            WaitStatus::Exited(_, code) if code != 0 => {
                logging::warn!(r#type = "prototype", worker_pid = %pid, code, "worker exited with non-zero code")
            }
            _ => {}
        }
    });
}

/// For the prototype's inherited copy; master keeps its own fd async.
pub fn clear_nonblocking(fd: RawFd) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    let fd = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags =
        fcntl(fd, FcntlArg::F_GETFL).map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
    let mut flags = OFlag::from_bits_truncate(flags);
    flags.remove(OFlag::O_NONBLOCK);
    fcntl(fd, FcntlArg::F_SETFL(flags)).map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
    Ok(())
}

/// Without this, `recv_command` blocks until a `SPAWN` that may never come,
/// and any worker dying in that window zombies until it does. Callers treat
/// `WouldBlock` as nothing to do rather than an error.
pub fn set_recv_timeout(fd: RawFd, timeout: Duration) -> std::io::Result<()> {
    let tv = libc::timeval {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_usec: timeout.subsec_micros() as libc::suseconds_t,
    };
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
