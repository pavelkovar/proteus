//! The prototype process: fork+exec'd from master with privileges already
//! dropped, single-threaded, no tokio. Initializes PHP once, then forks
//! workers on demand so each inherits that state without an exec.

pub mod php_ffi;

use crate::config::PhpOptions;
use crate::ipc::{CONFIG_FD, CONTROL_FD, control, shm};
use crate::logging;
use crate::utils::proctitle;
use crate::worker;
use nix::sys::socket::{AddressFamily, SockFlag, SockType, getsockopt, socketpair, sockopt};
use nix::unistd::{ForkResult, fork};
use php_ffi::PhpConn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::os::fd::{BorrowedFd, FromRawFd};
use std::os::unix::net::UnixStream as StdUnixStream;

/// Shared so the spawn side and the dispatch check cannot drift apart.
pub const INTERNAL_PROTOTYPE_ARG: &str = "--internal-prototype";

/// A worker's exit interrupts the blocking control read (SIGCHLD without
/// `SA_RESTART`); this only bounds a SIGCHLD that lands just before it.
const ZOMBIE_REAP_FALLBACK: std::time::Duration = std::time::Duration::from_secs(60);

extern "C" fn on_sigchld(_: libc::c_int) {}

fn interrupt_reads_on_sigchld() {
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = on_sigchld as *const () as usize;
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
        assert_eq!(
            libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()),
            0,
            "sigaction(SIGCHLD) failed"
        );
    }
}

/// Sent whole over `CONFIG_FD`, not argv - see that constant's doc.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub(crate) struct ProtoConfig {
    pub(crate) php_mod_path: String,
    pub(crate) max_requests: u32,
    pub(crate) options: PhpOptions,
    #[serde(default)]
    pub(crate) environment: HashMap<String, String>,
    #[serde(default)]
    pub(crate) log_level: u8,
}

/// Not a privilege check: it makes a manual invocation fail loudly rather
/// than strangely.
fn verify_control_fd_is_genuine() {
    let fd = unsafe { BorrowedFd::borrow_raw(CONTROL_FD) };
    if !is_seqpacket_socket(fd) {
        eprintln!(
            "[prototype] refusing to start: fd {CONTROL_FD} is not a SOCK_SEQPACKET control \
             socket - this process must be launched by its own master (master::prototype_launch::spawn), not \
             invoked directly"
        );
        std::process::exit(1);
    }
}

fn is_seqpacket_socket(fd: BorrowedFd) -> bool {
    matches!(getsockopt(&fd, sockopt::SockType), Ok(SockType::SeqPacket))
}

/// Seqpacket so a spilled body's fd arrives as one message. Close-on-exec:
/// master reads EOF as the worker's exit, which an exec()'d process would delay.
fn worker_link_pair() -> nix::Result<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    socketpair(
        AddressFamily::Unix,
        SockType::SeqPacket,
        None,
        SockFlag::SOCK_CLOEXEC,
    )
}

/// Forked, not yet reaped: the only pids safe to signal, since an unreaped pid
/// cannot be reused and reaping happens in this same single-threaded loop.
#[derive(Default)]
pub(crate) struct Children(std::collections::HashSet<i32>);

impl Children {
    pub(crate) fn forked(&mut self, pid: nix::unistd::Pid) {
        self.0.insert(pid.as_raw());
    }

    pub(crate) fn reaped(&mut self, pid: nix::unistd::Pid) {
        self.0.remove(&pid.as_raw());
    }

    pub(crate) fn kill(&self, pid: u32) {
        let Ok(raw) = i32::try_from(pid) else {
            return;
        };
        if !self.0.contains(&raw) {
            logging::debug!(
                r#type = "prototype",
                worker_pid = pid,
                "asked to kill a pid that is no longer a live worker, ignoring"
            );
            return;
        }
        if let Err(e) = crate::utils::process::kill_tolerating_esrch(
            nix::unistd::Pid::from_raw(raw),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            logging::warn!(r#type = "prototype", worker_pid = pid, error = %e, "signal delivery failed");
        }
    }
}

enum Prepared {
    Parent {
        fds: control::WorkerReadyFds,
        child: nix::unistd::Pid,
    },
    Child {
        link: std::os::fd::OwnedFd,
        mapped_channel: shm::MappedChannel,
        notify_efds: shm::NotifyEfds,
    },
}

/// Errors instead of panicking: a dead prototype takes every running worker
/// down with it (PDEATHSIG).
fn prepare_worker() -> std::io::Result<Prepared> {
    let (channel_fd, mapped_channel) = shm::create_channel()?;
    // Master parks on these rather than the ring's futex word.
    let notify_efds = shm::NotifyEfds {
        req_space: shm::create_notify_eventfd()?,
        resp_data: shm::create_notify_eventfd()?,
    };
    let (link_master_side, link_worker_side) = worker_link_pair()?;
    match unsafe { fork() }? {
        ForkResult::Child => {
            // PHP in a worker expects the default: a handler would interrupt its
            // own blocking calls whenever a process it started exits.
            unsafe { libc::signal(libc::SIGCHLD, libc::SIG_DFL) };
            drop(link_master_side);
            drop(channel_fd);
            Ok(Prepared::Child {
                link: link_worker_side,
                mapped_channel,
                notify_efds,
            })
        }
        ForkResult::Parent { child } => {
            drop(link_worker_side);
            // Unmaps only this view; the worker keeps its own.
            drop(mapped_channel);
            Ok(Prepared::Parent {
                fds: control::WorkerReadyFds {
                    channel: channel_fd,
                    notify: notify_efds,
                    link: link_master_side,
                },
                child,
            })
        }
    }
}

/// Entry point when re-exec'd as `--internal-prototype`.
pub fn run() -> ! {
    verify_control_fd_is_genuine();
    crate::logging::init(false);
    proctitle::set_title(&format!("{}: php prototype", crate::APP_NAME));
    control::clear_nonblocking(CONTROL_FD).expect("clear_nonblocking on control fd failed");
    control::set_recv_timeout(CONTROL_FD, ZOMBIE_REAP_FALLBACK)
        .expect("set_recv_timeout on control fd failed");
    interrupt_reads_on_sigchld();

    // Blocks until EOF, which is how master delimits the payload.
    let mut config_bytes = Vec::new();
    unsafe { std::fs::File::from_raw_fd(CONFIG_FD) }
        .read_to_end(&mut config_bytes)
        .expect("failed to read prototype config from CONFIG_FD");
    let proto_config: ProtoConfig =
        serde_json::from_slice(&config_bytes).expect("bad prototype config read from CONFIG_FD");
    let ProtoConfig {
        php_mod_path,
        max_requests,
        options: proto_options,
        environment: proto_environment,
        log_level,
    } = proto_config;
    logging::set_min_level(log_level);

    logging::info!(
        r#type = "prototype",
        pid = std::process::id(),
        "loading php-mod: {php_mod_path}"
    );

    // Before any fork(), so every worker inherits them. Reaches getenv()
    // but deliberately not $_ENV, which would import the whole environment
    // rather than these keys - the operator's call, not this server's.
    for (k, v) in &proto_environment {
        // Mutating the environment races `getenv()` on some platforms; safe
        // here only because nothing else is running yet.
        unsafe {
            std::env::set_var(k, v);
        }
    }

    let to_entries = |m: &HashMap<String, String>| -> Vec<String> {
        m.iter().map(|(k, v)| format!("{k}={v}")).collect()
    };
    let admin_entries = to_entries(&proto_options.admin);

    let phpconn = PhpConn::load(&php_mod_path).expect("dlopen php-mod failed");
    phpconn
        .init(&admin_entries, &to_entries(&proto_options.user))
        .expect("proteus_php_mod_init failed");
    logging::debug!(
        r#type = "prototype",
        "PHP embed SAPI + OPcache/APCu initialized, entering fork-server loop"
    );

    let mut control_stream = unsafe { StdUnixStream::from_raw_fd(CONTROL_FD) };
    let mut children = Children::default();

    loop {
        control::reap_finished_workers(|pid| children.reaped(pid));

        let cmd = match control::recv_command(&mut control_stream) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                logging::info!(
                    r#type = "prototype",
                    "control channel closed by master, exiting"
                );
                break;
            }
            // A worker exited, or the fallback timeout: reap and read again.
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(e) => {
                logging::error!(r#type = "prototype", error = %e, "control read error, exiting");
                break;
            }
        };

        match control::parse_command(&cmd) {
            control::Command::Spawn => {}
            control::Command::Kill(pid) => {
                children.kill(pid);
                continue;
            }
            control::Command::Unknown => {
                logging::error!(r#type = "prototype", command = ?cmd, "unknown control command");
                continue;
            }
        }

        let prototype_pid = nix::unistd::getpid();
        let (fds, child) = match prepare_worker() {
            Ok(Prepared::Parent { fds, child }) => (fds, child),
            Ok(Prepared::Child {
                link,
                mapped_channel,
                notify_efds,
            }) => {
                // A worker has no use for the prototype's control channel,
                // and std::process::exit below skips Drop, so nothing else
                // would ever close it.
                unsafe { libc::close(CONTROL_FD) };
                proctitle::set_title(&format!("{}: php worker", crate::APP_NAME));
                worker::run(
                    link,
                    mapped_channel,
                    &phpconn,
                    max_requests,
                    notify_efds,
                    prototype_pid,
                );
                std::process::exit(0);
            }
            // Refused, not fatal: the running workers are unaffected.
            Err(e) => {
                logging::error!(r#type = "prototype", error = %e, "could not make a worker");
                if let Err(e) = control::send_spawn_failed(&mut control_stream, &e) {
                    logging::error!(r#type = "prototype", error = %e, "reporting the failed spawn failed");
                }
                continue;
            }
        };
        children.forked(child);
        if let Err(e) = control::send_worker_ready(&mut control_stream, child.as_raw(), fds) {
            // Master will never use this worker, so stop it here.
            logging::error!(r#type = "prototype", error = %e, "send_worker_ready failed");
            children.kill(child.as_raw() as u32);
            if let Err(e) = control::send_spawn_failed(&mut control_stream, &e) {
                logging::error!(r#type = "prototype", error = %e, "reporting the failed spawn failed");
            }
        }
    }

    std::process::exit(0);
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
