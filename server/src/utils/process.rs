//! OS-level process control shared by every caller that drops privileges or
//! signals a possibly-already-dead target.

pub(crate) fn resolve_user(name: &str) -> nix::unistd::User {
    nix::unistd::User::from_name(name)
        .expect("getpwnam failed")
        .unwrap_or_else(|| panic!("user {name:?} not found"))
}

pub(crate) fn resolve_group(name: &str) -> nix::unistd::Group {
    nix::unistd::Group::from_name(name)
        .expect("getgrnam failed")
        .unwrap_or_else(|| panic!("group {name:?} not found"))
}

/// `ESRCH` (no such process) means the target already exited - the caller's
/// goal is already met, so that case is `Ok`, not an error to log.
pub(crate) fn kill_tolerating_esrch(
    pid: nix::unistd::Pid,
    sig: nix::sys::signal::Signal,
) -> Result<(), nix::errno::Errno> {
    match nix::sys::signal::kill(pid, sig) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(e) => Err(e),
    }
}
