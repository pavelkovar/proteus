//! The worker process: forked, never exec'd, from an already-initialized
//! prototype, so it inherits the SAPI and OPcache/APCu state for free.
//! Single-threaded, no tokio.

use crate::ipc::data;
use crate::ipc::shm;
use crate::logging;
use crate::prototype::php_ffi::{PhpChunk, PhpConn};
use std::os::fd::OwnedFd;

/// Bounds how much a response coalesces before being flushed, so a large
/// one streams in fixed steps no matter how PHP chose to call `ub_write`.
/// The cost is that small, gapped writes wait for the buffer to fill.
pub(crate) const COALESCE_FLUSH_THRESHOLD: usize = 64 * 1024;

/// All the loop below needs from PHP, as a trait so that the loop's own side
/// of the protocol can be exercised without a PHP runtime behind it.
pub(crate) trait ExecuteFile {
    fn execute_file(
        &self,
        script_path: &str,
        req: &data::PhpRequest<'_>,
        body_fd: Option<std::os::fd::BorrowedFd<'_>>,
        client_gone: &std::sync::atomic::AtomicBool,
        on_chunk: &mut dyn FnMut(PhpChunk),
    ) -> crate::prototype::php_ffi::ExecuteResult;
}

impl ExecuteFile for PhpConn {
    fn execute_file(
        &self,
        script_path: &str,
        req: &data::PhpRequest<'_>,
        body_fd: Option<std::os::fd::BorrowedFd<'_>>,
        client_gone: &std::sync::atomic::AtomicBool,
        on_chunk: &mut dyn FnMut(PhpChunk),
    ) -> crate::prototype::php_ffi::ExecuteResult {
        PhpConn::execute_file(self, script_path, req, body_fd, client_gone, on_chunk)
    }
}

/// Last-resort orphan guard: once master and the prototype both exit, a
/// parked worker would be reparented to init and wait forever on a ring
/// nobody will write to, holding its whole PHP heap.
///
/// The `getppid()` recheck closes the fork/prctl race, where the signal
/// would have been dispatched to the old parent. Best-effort: failing this
/// is not a reason to refuse to serve requests.
#[cfg(target_os = "linux")]
fn die_with_parent(expected_parent: nix::unistd::Pid) {
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        logging::warn!(
            r#type = "worker",
            error = %std::io::Error::last_os_error(),
            "prctl(PR_SET_PDEATHSIG) failed - this worker will not die with its prototype"
        );
        return;
    }
    if nix::unistd::getppid() != expected_parent {
        std::process::exit(0);
    }
}

#[cfg(not(target_os = "linux"))]
fn die_with_parent(_expected_parent: nix::unistd::Pid) {}

/// Serves requests until `max_requests` or the peer goes away.
///
/// `link` is held for the worker's whole life, so its process exit is the EOF
/// master watches for. `prototype_pid` must be read before the `fork()` - see
/// `die_with_parent`.
pub(crate) fn run(
    link: OwnedFd,
    channel: shm::MappedChannel,
    phpconn: &impl ExecuteFile,
    max_requests: u32,
    notify: shm::NotifyEfds,
    prototype_pid: nix::unistd::Pid,
) {
    use std::os::fd::AsRawFd;
    // Before the worker can park on anything untimed.
    die_with_parent(prototype_pid);
    // The owning `OwnedFd`s must outlive these raw numbers, or the fd could
    // be closed and silently aliased by something opened later.
    let req_space_efd_raw = notify.req_space.as_raw_fd();
    let resp_data_efd_raw = notify.resp_data.as_raw_fd();
    let pid = std::process::id();
    let channel = channel.channel();
    let mut served = 0u32;
    // Reused for the worker's whole life, in both directions.
    let mut scratch = data::RingScratch::default();

    loop {
        // At the top, not the tail: `req` borrows `scratch.read` for the rest
        // of the iteration, and a worker parked below should hold the shrunk
        // capacity rather than its last peak.
        scratch.shrink();

        let req = match data::read_request_from_ring(
            &channel.request,
            &channel.peer_death,
            &mut scratch.read,
            req_space_efd_raw,
        ) {
            Ok(Some(req)) => req,
            Ok(None) => break,
            Err(e) => {
                logging::warn!(r#type = "worker", pid, error = %e, "read_request_from_ring failed");
                break;
            }
        };
        // Master sends the fd only after the frame it belongs to is on the
        // ring, so by the time this runs it is either here or on its way.
        let body_fd = match req.body {
            data::RequestBody::File { .. } => match recv_body_fd(&link) {
                Ok(fd) => Some(fd),
                Err(e) => {
                    logging::warn!(r#type = "worker", pid, error = %e, "no fd arrived for a spilled request body");
                    break;
                }
            },
            data::RequestBody::Inline(_) => None,
        };
        served += 1;
        // Master raises this for the request it was watching; this one has a
        // client of its own.
        channel
            .client_gone
            .store(false, std::sync::atomic::Ordering::Release);
        // Computed before execute_file, because `End` can fire well ahead of
        // its return via fastcgi_finish_request() and must carry this.
        let retiring = served >= max_requests;

        // Once a write fails every later one fails identically, so this only
        // keeps a large response from logging once per remaining chunk.
        let mut write_failed = false;
        let write_scratch = &mut scratch.write;
        let mut on_chunk = |chunk: PhpChunk| {
            if write_failed {
                return;
            }
            let result = match chunk {
                PhpChunk::Headers { status, headers } => data::write_headers_to_ring(
                    &channel.response,
                    &channel.peer_death,
                    status,
                    &headers,
                    write_scratch,
                    resp_data_efd_raw,
                ),
                // Split before framing, or a single huge echo() would build
                // one frame larger than the ring can hold.
                PhpChunk::Body(bytes) => data::write_body_to_ring(
                    &channel.response,
                    &channel.peer_death,
                    bytes,
                    COALESCE_FLUSH_THRESHOLD,
                    write_scratch,
                    resp_data_efd_raw,
                ),
                PhpChunk::End => data::write_response_frame_to_ring(
                    &channel.response,
                    &channel.peer_death,
                    &data::ResponseFrameRef::End { retiring },
                    write_scratch,
                    resp_data_efd_raw,
                ),
            };
            if let Err(e) = result {
                logging::warn!(r#type = "worker", pid, error = %e, "write_response_frame_to_ring failed");
                write_failed = true;
            }
        };
        let result = phpconn.execute_file(
            &req.script_path,
            &req,
            body_fd.as_ref().map(std::os::fd::AsFd::as_fd),
            &channel.client_gone,
            &mut on_chunk,
        );

        // Unconditional, right after execute_file truly returns: this marker
        // is the only thing that tells master the worker is free again.
        if result.early_sent {
            logging::debug!(
                r#type = "worker",
                pid,
                "fastcgi_finish_request() fired, response already streamed early"
            );
        }
        if let Err(e) = data::write_worker_done_to_ring(
            &channel.response,
            &channel.peer_death,
            resp_data_efd_raw,
        ) {
            logging::warn!(r#type = "worker", pid, error = %e, "write_worker_done_to_ring failed");
            break;
        }

        if retiring {
            logging::debug!(
                r#type = "worker",
                pid,
                max_requests,
                "hit max_requests, retiring"
            );
            break;
        }
    }
}

/// The body fd master sent for this request. Blocking: master has already
/// written the frame, so it is sending or has sent.
pub(crate) fn recv_body_fd(socket: &OwnedFd) -> std::io::Result<OwnedFd> {
    use nix::sys::socket::{ControlMessageOwned, MsgFlags, recvmsg};
    use std::os::fd::{AsRawFd, FromRawFd};
    let mut buf = [0u8; 8];
    let mut iov = [std::io::IoSliceMut::new(&mut buf)];
    let mut cmsg = nix::cmsg_space!([std::os::fd::RawFd; 1]);
    let msg = recvmsg::<()>(
        socket.as_raw_fd(),
        &mut iov,
        Some(&mut cmsg),
        MsgFlags::empty(),
    )
    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
    for c in msg
        .cmsgs()
        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?
    {
        if let ControlMessageOwned::ScmRights(fds) = c
            && let Some(&fd) = fds.first()
        {
            return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "body message carried no fd",
    ))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
