//! Resolves the identity jobs run as and spawns/signals them.

use crate::logging;
use crate::utils::process;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use std::collections::BTreeMap;
use std::ffi::OsString;
use tokio::process::{Child, Command};

/// `uid`/`gid` are `None` together: a job then inherits whatever identity
/// `proteus cron` itself runs as, matching `php.user`/`php.group`'s own
/// omit-both rule.
pub(crate) struct Identity {
    uid: Option<u32>,
    gid: Option<u32>,
    home: String,
    name: String,
    inherit_env: bool,
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
                    inherit_env: false,
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
                    inherit_env: false,
                }
            }
            _ => panic!("--user and --group must be set together or not at all"),
        }
    }

    pub(crate) fn inheriting_env(self, inherit_env: bool) -> Identity {
        Identity {
            inherit_env,
            ..self
        }
    }
}

pub(crate) const DEFAULT_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

/// Lowest precedence first: defaults, `inherited`, the job's real identity and
/// shell (an inherited env may belong to another user), then the crontab.
fn job_environment(
    identity: &Identity,
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
    crontab: &[(String, String)],
) -> BTreeMap<OsString, OsString> {
    let mut env = BTreeMap::new();
    env.insert("PATH".into(), DEFAULT_PATH.into());
    if let Some(tz) = std::env::var_os("TZ") {
        env.insert("TZ".into(), tz);
    }
    env.extend(inherited);
    env.insert("HOME".into(), identity.home.clone().into());
    env.insert("LOGNAME".into(), identity.name.clone().into());
    env.insert("USER".into(), identity.name.clone().into());
    env.insert("SHELL".into(), "/bin/sh".into());
    env.extend(
        crontab
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
    );
    env
}

/// A spawned job. `child` must only be touched through the explicit
/// signal/reap sequence in `scheduler.rs` - not dropped or killed directly,
/// which would race that sequence (see `kill_on_drop(false)` below).
pub(crate) struct RunningJob {
    pub(crate) child: Child,
    pub(crate) pgid: Pid,
}

/// In its own process group, so a signal reaches `sh`'s children too.
pub(crate) fn spawn(
    identity: &Identity,
    command: &str,
    crontab_env: &[(String, String)],
) -> std::io::Result<RunningJob> {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(command);
    if let (Some(uid), Some(gid)) = (identity.uid, identity.gid) {
        cmd.uid(uid).gid(gid);
    }
    cmd.env_clear();
    let inherited: Vec<_> = if identity.inherit_env {
        std::env::vars_os().collect()
    } else {
        Vec::new()
    };
    cmd.envs(job_environment(identity, inherited, crontab_env));
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
