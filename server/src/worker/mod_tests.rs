//! Drives the real loop over a real channel, with PHP replaced by a closure.
//! What is under test is the loop's side of the protocol, not anything PHP
//! does.

use super::*;
use crate::ipc::data::{HeaderBlob, PhpRequest, RequestBody, ResponseFrame};
use crate::prototype::php_ffi::ExecuteResult;
use std::borrow::Cow;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;

/// PHP stands in as a closure: it gets the abort flag and a sink for chunks.
struct FakePhp<F>(F);

impl<F> ExecuteFile for FakePhp<F>
where
    F: Fn(&AtomicBool, &mut dyn FnMut(PhpChunk)),
{
    fn execute_file(
        &self,
        _script_path: &str,
        _req: &PhpRequest<'_>,
        _body_fd: Option<std::os::fd::BorrowedFd<'_>>,
        client_gone: &AtomicBool,
        on_chunk: &mut dyn FnMut(PhpChunk),
    ) -> ExecuteResult {
        (self.0)(client_gone, on_chunk);
        ExecuteResult { early_sent: false }
    }
}

fn empty_request() -> PhpRequest<'static> {
    PhpRequest {
        script_path: Cow::Borrowed("/srv/index.php"),
        document_root: Cow::Borrowed("/srv"),
        script_name: Cow::Borrowed("/index.php"),
        path_info: Cow::Borrowed(""),
        method: Cow::Borrowed("GET"),
        uri: Cow::Borrowed("/"),
        headers: HeaderBlob::default(),
        client_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        body: RequestBody::Inline(Cow::Borrowed(b"")),
        server_name: Cow::Borrowed("localhost"),
        server_addr: std::net::IpAddr::from([127, 0, 0, 1]),
        server_port: 80,
        server_protocol: Cow::Borrowed("HTTP/1.1"),
        https: false,
    }
}

/// Master's half of one channel: publishes requests and drains responses
/// through the same calls the real master uses.
struct MasterSide {
    mapped: shm::MappedChannel,
    req_space: AsyncFd<OwnedFd>,
}

impl MasterSide {
    async fn send_request(&self, req: &PhpRequest<'_>) {
        let mut scratch = Vec::new();
        let encoded = crate::ipc::data::encode_request(&mut scratch, req).unwrap();
        let channel = self.mapped.channel();
        crate::ipc::data::write_request_to_ring(
            &channel.request,
            &channel.peer_death,
            encoded,
            &self.req_space,
        )
        .await
        .expect("the request ring has room for one small request");
    }

    /// Polls rather than parking on the response eventfd: the worker runs on
    /// its own thread, so yielding is enough and the harness needs no second
    /// registration.
    async fn next_frame(&self, scratch: &mut Vec<u8>) -> Option<ResponseFrame<'static>> {
        let channel = self.mapped.channel();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "worker produced no frame in time"
            );
            if channel
                .response
                .try_read_frame(scratch, &channel.peer_death)
                .expect("the worker must not corrupt the response ring")
            {
                if scratch.is_empty() {
                    return None; // worker-done marker
                }
                let frame: ResponseFrame<'_> = postcard::from_bytes(scratch).unwrap();
                return Some(frame.into_owned());
            }
            tokio::task::yield_now().await;
        }
    }

    /// Reads to the worker-done marker, returning the `retiring` flag `End`
    /// carried.
    async fn drain_one_response(&self) -> bool {
        let mut scratch = Vec::new();
        let mut retiring = None;
        loop {
            match self.next_frame(&mut scratch).await {
                Some(ResponseFrame::End { retiring: r }) => retiring = Some(r),
                Some(_) => {}
                None => return retiring.expect("a response must carry exactly one End"),
            }
        }
    }

    fn mark_client_gone(&self) {
        self.mapped
            .channel()
            .client_gone
            .store(true, Ordering::Release);
    }
}

/// Runs the real loop on its own thread, since it blocks on the ring.
fn spawn_worker<F>(max_requests: u32, php: F) -> (MasterSide, std::thread::JoinHandle<()>)
where
    F: Fn(&AtomicBool, &mut dyn FnMut(PhpChunk)) + Send + 'static,
{
    let (fd, worker_mapping) = shm::create_channel().unwrap();
    let master_mapping = shm::map_existing_channel(fd).unwrap();
    let notify = shm::NotifyEfds {
        req_space: shm::create_notify_eventfd().unwrap(),
        resp_data: shm::create_notify_eventfd().unwrap(),
    };
    let worker_notify = notify.try_clone().unwrap();
    let (master_link, worker_link) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::SeqPacket,
        None,
        nix::sys::socket::SockFlag::empty(),
    )
    .unwrap();

    let handle = std::thread::spawn(move || {
        super::run(
            worker_link,
            worker_mapping,
            &FakePhp(php),
            max_requests,
            worker_notify,
            // The real parent, or `die_with_parent`'s recheck sees a
            // mismatch and exits the whole test process on the spot.
            nix::unistd::getppid(),
        );
    });
    drop(master_link);
    (
        MasterSide {
            mapped: master_mapping,
            req_space: AsyncFd::new(notify.req_space).unwrap(),
        },
        handle,
    )
}

/// `client_gone` is master's statement about the request it was watching. A
/// worker that carried it into the next request would abort scripts for
/// clients that are still there - every one of them, for the rest of its life.
#[tokio::test]
async fn client_gone_is_cleared_before_each_request() {
    let seen: Arc<std::sync::Mutex<Vec<bool>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let (master, handle) = spawn_worker(2, move |client_gone, emit| {
        recorded
            .lock()
            .unwrap()
            .push(client_gone.load(Ordering::Acquire));
        emit(PhpChunk::Headers {
            status: 200,
            headers: HeaderBlob::default(),
        });
        emit(PhpChunk::End);
    });

    let req = empty_request();
    master.send_request(&req).await;
    master.drain_one_response().await;

    // As master does when the client hangs up mid-response.
    master.mark_client_gone();

    master.send_request(&req).await;
    master.drain_one_response().await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![false, false],
        "the second request must start with a clear abort flag"
    );

    // Stops the worker whatever its retirement logic decided: a regression
    // there belongs to its own test rather than hanging this one.
    master.mapped.channel().mark_peer_dead();
    handle.join().unwrap();
}

/// `End` carries `retiring`, and master takes it as "no done marker is
/// coming". It has to be right at the moment `End` goes out, which after
/// `fastcgi_finish_request()` is long before the script returns.
#[tokio::test]
async fn the_last_request_announces_retirement_in_its_end_frame() {
    let (master, handle) = spawn_worker(2, |_client_gone, emit| {
        emit(PhpChunk::Headers {
            status: 200,
            headers: HeaderBlob::default(),
        });
        // Early, with work still to come - the shape of a script that called
        // fastcgi_finish_request().
        emit(PhpChunk::End);
        emit(PhpChunk::Body(b"ignored after End"));
    });

    let req = empty_request();
    master.send_request(&req).await;
    assert!(
        !master.drain_one_response().await,
        "a worker with requests left must not announce retirement"
    );

    master.send_request(&req).await;
    assert!(
        master.drain_one_response().await,
        "the request that reaches max_requests must say so in its own End frame"
    );

    handle
        .join()
        .expect("the worker must exit after its last request");
}

/// `max_requests: 0` means never recycle - a worker must keep serving well
/// past what would otherwise be its retirement point.
#[tokio::test]
async fn zero_max_requests_never_announces_retirement() {
    let (master, handle) = spawn_worker(0, |_client_gone, emit| {
        emit(PhpChunk::Headers {
            status: 200,
            headers: HeaderBlob::default(),
        });
        emit(PhpChunk::End);
    });

    let req = empty_request();
    for n in 0..5 {
        master.send_request(&req).await;
        assert!(
            !master.drain_one_response().await,
            "request {n} must not announce retirement with max_requests: 0"
        );
    }

    master.mapped.channel().mark_peer_dead();
    handle.join().unwrap();
}

/// Dropping master's end of the channel is how an abandoned worker is told to
/// stop; without it a parked one would hold its PHP heap forever.
#[tokio::test]
async fn a_parked_worker_exits_when_master_marks_the_peer_dead() {
    let (master, handle) = spawn_worker(100, |_client_gone, emit| {
        emit(PhpChunk::End);
    });

    master.mapped.channel().mark_peer_dead();
    handle
        .join()
        .expect("a parked worker must notice peer death");
}

/// A body big enough to spill arrives as `RequestBody::File`, with its fd
/// following separately over `link`. `spawn_worker` already drops master's
/// end of `link` before any request is sent, standing in for a master that
/// died between framing the request and sending its fd - the worker must
/// give up on the request rather than call into PHP with no body at all.
#[tokio::test]
async fn a_file_body_with_no_fd_on_the_link_ends_the_worker_without_calling_php() {
    let called = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&called);
    let (master, handle) = spawn_worker(2, move |_client_gone, _emit| {
        flag.store(true, Ordering::Release);
    });

    let mut req = empty_request();
    req.body = RequestBody::File { len: 4 };
    master.send_request(&req).await;

    handle
        .join()
        .expect("the worker must exit rather than hang with no fd ever arriving");
    assert!(
        !called.load(Ordering::Acquire),
        "PHP must never run for a request whose body fd never arrived"
    );
}
