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

/// Wakes the fork-server loop to reap finished workers even with no `SPAWN`
/// arriving.
const ZOMBIE_REAP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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

/// Entry point when re-exec'd as `--internal-prototype`.
pub fn run() -> ! {
    verify_control_fd_is_genuine();
    crate::logging::init(false);
    proctitle::set_title(&format!("{}: php prototype", crate::APP_NAME));
    control::clear_nonblocking(CONTROL_FD).expect("clear_nonblocking on control fd failed");
    control::set_recv_timeout(CONTROL_FD, ZOMBIE_REAP_INTERVAL)
        .expect("set_recv_timeout on control fd failed");

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

    loop {
        control::reap_finished_workers();

        let cmd = match control::recv_command(&mut control_stream) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                logging::info!(
                    r#type = "prototype",
                    "control channel closed by master, exiting"
                );
                break;
            }
            // The reap wakeup, with no SPAWN pending.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                logging::error!(r#type = "prototype", error = %e, "control read error, exiting");
                break;
            }
        };

        if cmd != control::SPAWN {
            logging::error!(r#type = "prototype", command = ?cmd, "unknown control command");
            continue;
        }

        // Mapped before the fork so the child inherits it. Failing loudly
        // beats a silent `continue`, which would hang the master already
        // blocked on WORKER_READY; a crashed prototype is respawned anyway.
        let (channel_fd, mapped_channel) =
            shm::create_channel().expect("failed to create worker data channel");
        // Master parks on these rather than the ring's futex word. Created
        // pre-fork, for the reason above.
        let notify_efds = shm::NotifyEfds {
            req_space: shm::create_notify_eventfd()
                .expect("failed to create request-space eventfd"),
            resp_data: shm::create_notify_eventfd()
                .expect("failed to create response-data eventfd"),
        };

        // Seqpacket, so one send is one receive: the ring cannot carry an fd,
        // and a spilled body has to arrive as exactly one message.
        let (link_master_side, link_worker_side) = socketpair(
            AddressFamily::Unix,
            SockType::SeqPacket,
            None,
            SockFlag::empty(),
        )
        .expect("failed to create the worker link socketpair");

        // Before the fork, where this is still the value `die_with_parent`
        // needs to compare `getppid()` against.
        let prototype_pid = nix::unistd::getpid();
        match unsafe { fork() }.expect("fork failed") {
            ForkResult::Child => {
                drop(link_master_side);
                // A worker has no use for the prototype's control channel,
                // and std::process::exit below skips Drop, so nothing else
                // would ever close it.
                unsafe { libc::close(CONTROL_FD) };
                proctitle::set_title(&format!("{}: php worker", crate::APP_NAME));
                worker::run(
                    link_worker_side,
                    mapped_channel,
                    &phpconn,
                    max_requests,
                    notify_efds,
                    prototype_pid,
                );
                std::process::exit(0);
            }
            ForkResult::Parent { child } => {
                drop(link_worker_side);
                // Unmaps only this view; the worker keeps its own.
                drop(mapped_channel);
                let fds = control::WorkerReadyFds {
                    channel: channel_fd,
                    notify: notify_efds,
                    link: link_master_side,
                };
                if let Err(e) = control::send_worker_ready(&mut control_stream, child.as_raw(), fds)
                {
                    logging::error!(r#type = "prototype", error = %e, "send_worker_ready failed");
                }
            }
        }
    }

    std::process::exit(0);
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
