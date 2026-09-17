//! Master's whole view of the prototype process: bringing it up (fork+exec,
//! privilege drop, the fixed-fd config handoff) and tracking it afterwards
//! (pid, reap state, kill, respawn).

use crate::ipc::{CONFIG_FD, CONTROL_FD};
use crate::logging;
use crate::prototype::{INTERNAL_PROTOTYPE_ARG, ProtoConfig};
use nix::errno::Errno;
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use std::io::Write;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::os::unix::process::CommandExt;
use std::process::Command;
use tokio_seqpacket::UnixSeqpacket;

#[derive(Clone)]
pub(crate) struct Spec {
    pub(crate) config: ProtoConfig,
    /// `None` must skip the `uid`/`gid` calls entirely, not pass master's own
    /// identity: `Command::uid` always triggers `setgroups(0, NULL)`, silently
    /// wiping supplementary groups even on a same-value drop.
    pub(crate) drop_to: Option<(u32, u32)>,
    pub(crate) no_new_privs: bool,
}

/// Moves `fd` above `floor` so a later `dup2` onto a fixed low number cannot
/// clobber it. The caller closes the result after the `dup2`s consuming it.
///
/// Must stay async-signal-safe: this runs between `fork` and `exec`.
unsafe fn relocate_above(
    fd: std::os::fd::RawFd,
    floor: std::os::fd::RawFd,
) -> std::io::Result<std::os::fd::RawFd> {
    let moved = unsafe { libc::fcntl(fd, libc::F_DUPFD, floor + 1) };
    if moved < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(moved)
}

/// Returns master's end of the control channel and the prototype's pid - see
/// `Handle` for why the `Child` is not handed out.
pub(crate) fn spawn(spec: &Spec) -> std::io::Result<(UnixSeqpacket, u32)> {
    let no_new_privs = spec.no_new_privs;
    let (master_end, prototype_end) = UnixSeqpacket::pair()?;
    let prototype_fd = prototype_end.into_raw_fd();

    // A plain `pipe()` leaves both ends inheritable, so a racing
    // `Command::spawn` would inherit them, and the write end leaking into an
    // unrelated child keeps CONFIG_FD from ever seeing EOF.
    let (config_read, config_write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)?;
    let config_read_fd = config_read.into_raw_fd();
    let config_write_fd = config_write.into_raw_fd();

    let exe = std::env::current_exe()?;
    let mut cmd = Command::new(exe);
    let config_json = serde_json::to_vec(&spec.config).unwrap_or_else(|_| b"{}".to_vec());
    cmd.arg(INTERNAL_PROTOTYPE_ARG);
    if let Some((uid, gid)) = spec.drop_to {
        cmd.uid(uid).gid(gid);
    }
    unsafe {
        cmd.pre_exec(move || {
            // Survives this `execve` and every `fork` below it, so one call
            // here covers the prototype and every worker it goes on to make.
            if no_new_privs && libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Both sources move clear of the target range before either
            // dup2. In place it is order-dependent: if the OS handed us
            // CONTROL_FD for the config pipe, the first dup2 would close the
            // config end and the second would install the control socket on
            // CONFIG_FD, leaving the prototype waiting on a config that never
            // arrives.
            let control_src = relocate_above(prototype_fd, CONFIG_FD)?;
            let config_src = relocate_above(config_read_fd, CONFIG_FD)?;
            if libc::dup2(control_src, CONTROL_FD) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::dup2(config_src, CONFIG_FD) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Only after both dup2s: closing earlier lets the second
            // relocation reuse the number the first one holds.
            libc::close(control_src);
            libc::close(config_src);
            // Leaking this into the child keeps CONFIG_FD from ever seeing
            // EOF, hanging the prototype's read forever.
            libc::close(config_write_fd);
            Ok(())
        });
    }

    // Every fd above must close on every path from here: respawn retries
    // repeatedly, and a leak per attempt exhausts the fd table.
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            unsafe {
                libc::close(prototype_fd);
                libc::close(config_read_fd);
                libc::close(config_write_fd);
            }
            return Err(e);
        }
    };
    // The child holds its own dup'd copies; keeping master's would break
    // EOF detection when the prototype dies.
    unsafe {
        libc::close(prototype_fd);
        libc::close(config_read_fd);
    }

    // Closing the write end is what ends the child's read; the handoff
    // carries no length prefix.
    let write_result = {
        let mut w = unsafe { std::fs::File::from_raw_fd(config_write_fd) };
        w.write_all(&config_json)
    };
    if let Err(e) = write_result {
        // The child is already running and would otherwise block forever on
        // an incomplete config. Left for master's own sweep to reap, which is
        // the only place allowed to wait on it.
        let _ = child.kill();
        return Err(e);
    }

    Ok((master_end, child.id()))
}

/// The prototype's pid and everything that may be done to it.
///
/// A pid rather than a `std::process::Child`: master reaps through
/// `waitpid(-1)`, which a `Child::wait` would race for the status.
///
/// An unreaped pid cannot be recycled, so signalling it stays safe until
/// something reaps it - which is what `reaped` tracks.
pub(super) struct Handle {
    pid: u32,
    reaped: bool,
}

impl Handle {
    pub(super) fn new(pid: u32) -> Self {
        Handle { pid, reaped: false }
    }

    pub(super) fn pid(&self) -> u32 {
        self.pid
    }

    /// `None` once reaped: the only safe answer to what may be signalled.
    pub(super) fn live_pid(&self) -> Option<u32> {
        (!self.reaped).then_some(self.pid)
    }

    /// Records a reaped child, reporting whether it was the prototype.
    pub(super) fn note_exit(&mut self, pid: u32, status: WaitStatus) -> bool {
        if self.reaped || pid != self.pid {
            return false;
        }
        self.reaped = true;
        log_prototype_death(pid, status);
        true
    }

    /// Its workers need no separate kill: they follow via `PR_SET_PDEATHSIG`.
    pub(super) fn kill(&self, context: &str) {
        if let Some(pid) = self.live_pid() {
            super::sigkill(pid, context);
        }
    }

    /// Kills and reaps whatever it replaces: not every respawn follows a
    /// death, and forgetting a wedged prototype's pid leaves it orphaned with
    /// its whole PHP heap, tracked by nothing.
    ///
    /// Reaps before killing, so a death that already happened keeps the status
    /// that explains it instead of reporting SIGKILL.
    pub(super) fn replace(&mut self, new_pid: u32) {
        if let Some(pid) = self.live_pid() {
            match waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG)) {
                Ok(status @ (WaitStatus::Exited(..) | WaitStatus::Signaled(..))) => {
                    log_prototype_death(pid, status);
                }
                // Reaped by something else, so the pid may name an unrelated
                // process by now.
                Err(Errno::ECHILD) => {}
                // A failed wait included: an unkilled prototype is the one
                // outcome that leaks a PHP heap.
                _ => {
                    super::sigkill(pid, "replacing a prototype that is still running");
                    let _ = waitpid(Pid::from_raw(pid as i32), None);
                }
            }
        }
        *self = Handle::new(new_pid);
    }
}

fn log_prototype_death(pid: u32, status: WaitStatus) {
    match status {
        WaitStatus::Exited(_, code) => {
            logging::error!(
                r#type = "controller",
                pid,
                exit_code = code,
                "prototype exited"
            )
        }
        WaitStatus::Signaled(_, signal, core_dumped) => logging::error!(
            r#type = "controller",
            pid,
            signal = signal.as_str(),
            core_dumped,
            "prototype was killed by a signal"
        ),
        other => {
            logging::error!(r#type = "controller", pid, status = ?other, "prototype stopped")
        }
    }
}

#[cfg(test)]
#[path = "prototype_tests.rs"]
mod tests;
