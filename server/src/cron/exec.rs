//! Resolves the identity jobs run as and spawns/signals them.

use crate::logging;
use crate::utils::process;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use tokio::process::{Child, Command};

/// `uid`/`gid` are `None` together: a job then inherits whatever identity
/// `proteus cron` itself runs as, matching `php.user`/`php.group`'s own
/// omit-both rule.
pub(crate) struct Identity {
    uid: Option<u32>,
    gid: Option<u32>,
    home: String,
    name: String,
}

impl Identity {
    pub(crate) fn resolve(user: Option<&str>, group: Option<&str>) -> Identity {
        match (user, group) {
            (Some(user), Some(group)) => {
                let u = process::resolve_user(user);
                let g = process::resolve_group(group);
                Identity {
                    uid: Some(u.uid.as_raw()),
                    gid: Some(g.gid.as_raw()),
                    home: u.dir.to_string_lossy().into_owned(),
                    name: u.name,
                }
            }
            (None, None) => {
                let u = nix::unistd::User::from_uid(nix::unistd::getuid())
                    .expect("getpwuid failed")
                    .unwrap_or_else(|| panic!("no passwd entry for the current uid"));
                Identity {
                    uid: None,
                    gid: None,
                    home: u.dir.to_string_lossy().into_owned(),
                    name: u.name,
                }
            }
            _ => panic!("--user and --group must be set together or not at all"),
        }
    }
}

/// A spawned job. `child` must only be touched through the explicit
/// signal/reap sequence in `scheduler.rs` - not dropped or killed directly,
/// which would race that sequence (see `kill_on_drop(false)` below).
pub(crate) struct RunningJob {
    pub(crate) child: Child,
    pub(crate) pgid: Pid,
}

/// `sh -c command`, with a fresh minimal environment (cronie's own default,
/// not `proteus cron`'s) and its own process group so a signal can reach
/// `sh`'s children too, not just `sh` itself.
pub(crate) fn spawn(identity: &Identity, command: &str) -> std::io::Result<RunningJob> {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(command);
    if let (Some(uid), Some(gid)) = (identity.uid, identity.gid) {
        cmd.uid(uid).gid(gid);
    }
    cmd.env_clear();
    cmd.env("HOME", &identity.home);
    cmd.env("LOGNAME", &identity.name);
    cmd.env("USER", &identity.name);
    cmd.env("SHELL", "/bin/sh");
    cmd.env("PATH", "/usr/bin:/bin");
    if let Ok(tz) = std::env::var("TZ") {
        cmd.env("TZ", tz);
    }
    cmd.current_dir(&identity.home);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::inherit());
    cmd.stderr(std::process::Stdio::inherit());
    // We signal and reap explicitly on shutdown; a drop mid-run must not
    // race that with its own kill.
    cmd.kill_on_drop(false);
    unsafe {
        cmd.pre_exec(|| {
            // `sh -c "a && b"` never execs over itself, so without its own
            // group a signal to it alone would miss `b`.
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let pid = child.id().expect("just-spawned child has a pid");
    Ok(RunningJob {
        child,
        pgid: Pid::from_raw(pid as i32),
    })
}

/// Signals the whole group, tolerating the `fork`/`setsid` race where the
/// group may not exist yet. Paired with a direct signal to the pid itself,
/// which still exists even inside that race.
pub(crate) fn signal_group(pgid: Pid, sig: Signal) {
    let group = Pid::from_raw(-pgid.as_raw());
    if let Err(e) = process::kill_tolerating_esrch(group, sig) {
        logging::warn!(r#type = "cron", error = %e, signal = %sig, "killpg failed");
    }
    let _ = process::kill_tolerating_esrch(pgid, sig);
}

#[cfg(test)]
#[path = "exec_tests.rs"]
mod tests;
