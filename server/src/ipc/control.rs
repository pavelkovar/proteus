//! Control channel (master <-> prototype), SOCK_SEQPACKET + SCM_RIGHTS.
//!
//! Master must close its own copy of every fd it dup2's into the child, or
//! EOF detection breaks when the prototype dies.

use crate::ipc::shm;
use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use std::io::{IoSlice, Read};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::time::Duration;
use tokio_seqpacket::ancillary::OwnedAncillaryMessage;
use tokio_seqpacket::UnixSeqpacket;

pub const SPAWN: &[u8] = b"SPAWN";

/// Named fields rather than a tuple, so two same-typed fds cannot be
/// transposed at a call site without a compile error.
pub struct WorkerReadyFds {
    pub channel: OwnedFd,
    pub liveness: OwnedFd,
    pub notify: shm::NotifyEfds,
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
    let [channel_fd, liveness_fd, req_space_efd, resp_data_efd] = <[OwnedFd; 4]>::try_from(fds).map_err(|fds| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected exactly 4 fds in WORKER_READY reply, got {}", fds.len()),
        )
    })?;

    Ok((
        WorkerReadyFds {
            channel: channel_fd,
            liveness: liveness_fd,
            notify: shm::NotifyEfds { req_space: req_space_efd, resp_data: resp_data_efd },
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

/// All four fds go in one `SCM_RIGHTS` message, in the fixed order
/// `request_worker` unpacks them.
pub fn send_worker_ready(control: &mut StdUnixStream, worker_pid: i32, fds: WorkerReadyFds) -> std::io::Result<()> {
    let mut payload = (worker_pid as u32).to_le_bytes().to_vec();
    payload.extend_from_slice(b"READY");

    let raw_fds =
        [fds.channel.as_raw_fd(), fds.liveness.as_raw_fd(), fds.notify.req_space.as_raw_fd(), fds.notify.resp_data.as_raw_fd()];
    let cmsg = [ControlMessage::ScmRights(&raw_fds)];
    let iov = [IoSlice::new(&payload)];

    sendmsg::<()>(control.as_raw_fd(), &iov, &cmsg, MsgFlags::empty(), None)
        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
    Ok(())
    // Closes the prototype's own copies; master holds its own duplicates.
}

/// Non-blocking, so zombies do not accumulate while waiting for a command.
pub fn reap_finished_workers() {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => break,
            // Turns a mystery 500 into a diagnosable one.
            Ok(WaitStatus::Signaled(pid, signal, _)) => {
                tracing::warn!(r#type = "prototype", worker_pid = %pid, ?signal, "worker killed by signal");
            }
            Ok(WaitStatus::Exited(pid, code)) if code != 0 => {
                tracing::warn!(r#type = "prototype", worker_pid = %pid, code, "worker exited with non-zero code");
            }
            Ok(_status) => continue,
            Err(nix::errno::Errno::ECHILD) => break,
            Err(_) => break,
        }
    }
}

/// For the prototype's inherited copy; master keeps its own fd async.
pub fn clear_nonblocking(fd: RawFd) -> std::io::Result<()> {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};
    let fd = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags = fcntl(fd, FcntlArg::F_GETFL).map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
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
