use super::*;
use crate::ipc::{data, shm};
use crate::master::worker_channel::WorkerChannel;
use std::os::fd::AsRawFd;
use tokio::io::unix::AsyncFd;

/// Out of range, so the kill a failed drain may attempt finds nothing.
const NO_REAL_WORKER_PID: u32 = 999_999_999;

/// A `WorkerChannel` plus the worker's own side of the same memfd, for
/// writing response frames without a second process.
fn channel_pair() -> (WorkerChannel, shm::MappedChannel, std::os::fd::RawFd) {
    let (fd, worker_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let resp_data_efd_owned = shm::create_notify_eventfd().unwrap();
    let resp_data_efd_raw = resp_data_efd_owned.as_raw_fd();
    let channel = WorkerChannel::for_test(
        NO_REAL_WORKER_PID,
        Arc::new(master_side),
        AsyncFd::new(shm::create_notify_eventfd().unwrap()).unwrap(),
        AsyncFd::new(resp_data_efd_owned).unwrap(),
    );
    (channel, worker_side, resp_data_efd_raw)
}

fn write_body(worker_side: &shm::MappedChannel, efd: std::os::fd::RawFd, bytes: &[u8]) {
    let channel = worker_side.channel();
    let mut scratch = Vec::new();
    data::write_response_frame_to_ring(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::Body(bytes),
        &mut scratch,
        efd,
    )
    .unwrap();
}

fn write_end(worker_side: &shm::MappedChannel, efd: std::os::fd::RawFd) {
    let channel = worker_side.channel();
    let mut scratch = Vec::new();
    data::write_response_frame_to_ring(
        &channel.response,
        &channel.peer_death,
        &data::ResponseFrameRef::End { retiring: false },
        &mut scratch,
        efd,
    )
    .unwrap();
}

#[tokio::test]
async fn a_short_finished_response_is_taken_whole() {
    let (mut channel, worker_side, efd) = channel_pair();
    write_body(&worker_side, efd, b"hello ");
    write_body(&worker_side, efd, b"world");
    write_end(&worker_side, efd);

    match PoolManager::drain_ready(&mut channel) {
        Drained::Complete { body, retiring } => {
            assert_eq!(body.as_ref(), b"hello world");
            assert!(!retiring);
        }
        _ => panic!("a response that is already finished must go out in one piece"),
    }
}

/// Every read frees ring space the worker can refill, so without a bound the
/// sweep follows a producer instead of returning - buffering a whole
/// response per in-flight request rather than streaming it.
#[tokio::test]
async fn a_worker_past_the_budget_is_streamed_rather_than_buffered_whole() {
    let (mut channel, worker_side, efd) = channel_pair();
    let chunk = vec![b'x'; 8 * 1024];
    let chunks = MAX_DRAINED_PREFIX_BYTES / chunk.len() + 1;
    for _ in 0..chunks {
        write_body(&worker_side, efd, &chunk);
    }
    write_end(&worker_side, efd);

    match PoolManager::drain_ready(&mut channel) {
        Drained::Pending { prefix } => {
            let drained: usize = prefix.iter().map(|c| c.len()).sum();
            assert!(
                drained < chunks * chunk.len(),
                "the sweep must stop short of the whole response, took {drained}"
            );
        }
        Drained::Complete { body, .. } => {
            panic!("{} bytes were buffered instead of streamed", body.len())
        }
        _ => panic!("unexpected drain outcome"),
    }
}

/// The single-frame case is the one worth not copying, so it must still
/// arrive whole.
#[tokio::test]
async fn a_response_the_worker_wrote_in_one_frame_is_taken_as_is() {
    let (mut channel, worker_side, efd) = channel_pair();
    write_body(&worker_side, efd, b"just the one");
    write_end(&worker_side, efd);

    match PoolManager::drain_ready(&mut channel) {
        Drained::Complete { body, .. } => assert_eq!(body.as_ref(), b"just the one"),
        _ => panic!("a finished one-frame response must go out whole"),
    }
}

/// A second `Headers` run after the first is a protocol violation, and the
/// bytes already read still have to reach the client ahead of the error.
#[tokio::test]
async fn a_second_headers_run_ends_the_sweep_with_what_it_had() {
    let (mut channel, worker_side, efd) = channel_pair();
    write_body(&worker_side, efd, b"before");

    let ring = worker_side.channel();
    let mut header_pairs = data::HeaderBlob::default();
    header_pairs.push("X-Second", "1");
    let mut scratch = Vec::new();
    data::write_response_frame_to_ring(
        &ring.response,
        &ring.peer_death,
        &data::ResponseFrameRef::Headers {
            status: 200,
            headers: (&header_pairs).into(),
            more: false,
        },
        &mut scratch,
        efd,
    )
    .unwrap();

    match PoolManager::drain_ready(&mut channel) {
        Drained::Broken { prefix, error } => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            let carried: Vec<u8> = prefix.iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(
                carried, b"before",
                "bytes read before the violation are lost"
            );
        }
        _ => panic!("a second Headers run must not be treated as a normal response"),
    }
}
