use super::*;
use std::alloc::{Layout, alloc};
use std::os::fd::AsRawFd;
use std::time::Duration;

/// Heap-allocated so a test can use a realistic capacity without overflowing
/// the stack constructing it.
fn make_ring<const N: usize>() -> &'static Ring<N> {
    unsafe {
        let ptr = alloc(Layout::new::<Ring<N>>()) as *mut Ring<N>;
        assert!(!ptr.is_null());
        Ring::init_in_place(ptr);
        &*ptr
    }
}

fn make_peer_death() -> &'static PeerDeath {
    unsafe {
        let ptr = alloc(Layout::new::<PeerDeath>()) as *mut PeerDeath;
        assert!(!ptr.is_null());
        PeerDeath::init_in_place(ptr);
        &*ptr
    }
}

/// For tests that never wait on it and only need a real fd.
fn make_notify_efd() -> OwnedFd {
    create_notify_eventfd().unwrap()
}

#[test]
fn write_read_roundtrip() {
    let ring: &Ring<64> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();

    ring.write_frame(b"hello", peer, efd.as_raw_fd()).unwrap();
    let mut scratch = Vec::new();
    ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap();
    assert_eq!(scratch, b"hello");
}

#[test]
fn multiple_frames_pipeline_without_reader() {
    let ring: &Ring<64> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();

    ring.write_frame(b"one", peer, efd.as_raw_fd()).unwrap();
    ring.write_frame(b"two", peer, efd.as_raw_fd()).unwrap();
    ring.write_frame(b"three", peer, efd.as_raw_fd()).unwrap();

    let mut scratch = Vec::new();
    ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap();
    assert_eq!(scratch, b"one");
    ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap();
    assert_eq!(scratch, b"two");
    ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap();
    assert_eq!(scratch, b"three");
}

#[test]
fn survives_wraparound() {
    // Small enough that the positions wrap many times over.
    let ring: &Ring<32> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();
    let mut scratch = Vec::new();

    for i in 0..1000u32 {
        let payload = i.to_le_bytes();
        ring.write_frame(&payload, peer, efd.as_raw_fd()).unwrap();
        ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
            .unwrap();
        assert_eq!(scratch.as_slice(), &payload);
    }
}

#[test]
fn write_frame_too_large_is_rejected_not_panicked() {
    let ring: &Ring<16> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();

    let oversized = vec![0u8; 64];
    let err = ring
        .write_frame(&oversized, peer, efd.as_raw_fd())
        .unwrap_err();
    assert!(matches!(err, RingError::FrameTooLarge));
}

#[test]
fn write_frame_exact_capacity_boundary() {
    // Exactly the largest payload that can fit, not merely close to it.
    let ring: &Ring<16> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();

    let one_over = vec![0u8; 13];
    assert!(matches!(
        ring.write_frame(&one_over, peer, efd.as_raw_fd()),
        Err(RingError::FrameTooLarge)
    ));

    let exact = vec![7u8; 12];
    ring.write_frame(&exact, peer, efd.as_raw_fd()).unwrap();
    let mut scratch = Vec::new();
    ring.read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap();
    assert_eq!(scratch, exact);
}

#[test]
fn read_frame_rejects_corrupt_length_prefix_not_panicked() {
    // A hostile peer's length prefix must error, never panic: this is the
    // master<->worker privilege boundary.
    let ring: &Ring<16> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();

    // Straight into the mapping and past `write_pos`, because a hostile peer
    // is another process scribbling on shared memory, not a caller of this API.
    unsafe { ring.copy_at(0, &9_999u32.to_le_bytes()) };
    ring.write_pos.store(LEN_PREFIX as u64, Ordering::Release);
    ring.notify_data_written();

    let mut scratch = Vec::new();
    let err = ring
        .read_frame(&mut scratch, peer, efd.as_raw_fd())
        .unwrap_err();
    assert!(matches!(err, RingError::FrameTooLarge));
}

#[test]
fn peer_death_wakes_blocked_reader() {
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();

    let handle = std::thread::spawn(move || {
        let mut scratch = Vec::new();
        ring.read_frame(&mut scratch, peer, efd_raw)
    });

    // Long enough for the reader to actually park.
    std::thread::sleep(Duration::from_millis(50));
    peer.mark_dead();
    ring.mark_dead_and_wake();

    let result = handle.join().unwrap();
    assert!(matches!(result, Err(RingError::PeerGone)));
}

#[test]
fn peer_death_wakes_blocked_writer() {
    // Leaves the writer no space, so it must block.
    let ring: &'static Ring<16> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    ring.write_frame(b"xxxxxxxx", peer, efd_raw).unwrap(); // 4-byte prefix + 8 bytes == capacity

    let handle = std::thread::spawn(move || ring.write_frame(b"y", peer, efd_raw));

    std::thread::sleep(Duration::from_millis(50));
    peer.mark_dead();
    ring.mark_dead_and_wake();

    let result = handle.join().unwrap();
    assert!(matches!(result, Err(RingError::PeerGone)));
}

// Deliberately no both-ends-blocking stress test: that pairing never occurs
// in production, one side of any ring always being master's async call, and
// it deadlocks - the eventfd side has nothing awaiting it, so the futex
// waiter opposite is never woken.

#[test]
fn notify_eventfd_is_gated_not_fired_on_every_frame() {
    // Sequential, so neither side ever parks - which is what makes this a
    // test that the notify fires only for a real waiter, keeping the common
    // path free of the syscall.
    let ring: &Ring<64> = make_ring();
    let peer = make_peer_death();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();

    for _ in 0..100 {
        ring.write_frame(b"x", peer, efd_raw).unwrap();
        let mut scratch = Vec::new();
        ring.read_frame(&mut scratch, peer, efd_raw).unwrap();
    }

    let mut val: u64 = 0;
    let n = unsafe { libc::read(efd_raw, &mut val as *mut u64 as *mut libc::c_void, 8) };
    let err = std::io::Error::last_os_error();
    assert_eq!(
        n, -1,
        "eventfd should have no pending count - nobody ever parked on it"
    );
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn create_and_map_existing_channel_share_the_same_memory() {
    // Two independent mappings of one memfd, as the real cross-process setup
    // has, which must observe each other's writes.
    let (fd, prototype_side) = create_channel().unwrap();
    let master_side = map_existing_channel(fd).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();

    let proto_peer = &prototype_side.channel().peer_death;
    prototype_side
        .channel()
        .request
        .write_frame(b"from prototype", proto_peer, efd_raw)
        .unwrap();

    let master_peer = &master_side.channel().peer_death;
    let mut scratch = Vec::new();
    master_side
        .channel()
        .request
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    assert_eq!(scratch, b"from prototype");

    master_side
        .channel()
        .response
        .write_frame(b"from master", master_peer, efd_raw)
        .unwrap();
    prototype_side
        .channel()
        .response
        .read_frame(&mut scratch, proto_peer, efd_raw)
        .unwrap();
    assert_eq!(scratch, b"from master");
}

/// The invariant this publish scheme exists for: a frame `write_pos` covers is
/// already there in full.
///
/// Two failures to catch, so two checks. An observer spins on `write_pos` and
/// rejects any value inside a frame - that is the split publish. The reader
/// verifies every payload byte - that is a publish racing ahead of the copies.
///
/// The payload is large on purpose: it stretches the gap between writing the
/// length and finishing the payload from nanoseconds to microseconds, so a
/// producer descheduled inside that window is the common case here rather
/// than a rare one.
#[tokio::test]
async fn a_published_frame_is_never_visible_before_its_payload() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    const PAYLOAD: usize = 32 * 1024;
    const FRAME: u64 = (LEN_PREFIX + PAYLOAD) as u64;
    const FRAMES: usize = 300;

    // The real pairing only - blocking on both ends deadlocks, see this
    // module's own note above.
    let ring: &'static Ring<262144> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let data_efd_owned = make_notify_efd();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicBool::new(false));
    let observer = std::thread::spawn({
        let (done, torn) = (Arc::clone(&done), Arc::clone(&torn));
        move || {
            let mut samples = 0u64;
            while !done.load(Ordering::Relaxed) {
                if !ring.write_pos.load(Ordering::Acquire).is_multiple_of(FRAME) {
                    torn.store(true, Ordering::Relaxed);
                }
                samples += 1;
            }
            samples
        }
    });

    let writer = std::thread::spawn(move || {
        for i in 0..FRAMES {
            ring.write_frame(&[i as u8; PAYLOAD], peer, data_efd_raw)
                .unwrap();
        }
    });

    let mut scratch = Vec::new();
    for i in 0..FRAMES {
        tokio::time::timeout(
            Duration::from_secs(10),
            ring.read_frame_async(&mut scratch, peer, &data_efd),
        )
        .await
        .expect("should not hang - see this test's own doc comment")
        .unwrap();
        assert_eq!(scratch.len(), PAYLOAD, "frame {i} arrived truncated");
        assert!(
            scratch.iter().all(|&b| b == i as u8),
            "frame {i} was published before its bytes landed"
        );
    }
    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();

    done.store(true, Ordering::Relaxed);
    let samples = observer.join().unwrap();
    assert!(
        samples > FRAMES as u64,
        "observer sampled {samples} times - too coarse to conclude anything"
    );
    assert!(
        !torn.load(Ordering::Relaxed),
        "write_pos was published inside a frame"
    );
}

// --- Response-ring reclaim (`fallocate(FALLOC_FL_PUNCH_HOLE)`) ---

/// The worker side holds no fd and has nothing to reclaim, so this must
/// short-circuit rather than attempt a fallocate.
#[test]
fn reclaim_response_is_a_noop_on_the_workers_own_mapping() {
    let (fd, prototype_side) = create_channel().unwrap();
    let _master_side = map_existing_channel(fd).unwrap(); // keeps the memfd alive

    assert!(!prototype_side.reclaim_is_due());
    prototype_side.reclaim_if_due(); // must not panic
}

#[test]
fn reclaim_response_below_threshold_is_a_noop() {
    let (fd, prototype_side) = create_channel().unwrap();
    let master_side = map_existing_channel(fd).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    let proto_peer = &prototype_side.channel().peer_death;
    let master_peer = &master_side.channel().peer_death;

    // Under the threshold: the common case must never pay for a fallocate.
    prototype_side
        .channel()
        .response
        .write_frame(b"tiny", proto_peer, efd_raw)
        .unwrap();
    prototype_side
        .channel()
        .response
        .write_frame(&[], proto_peer, efd_raw)
        .unwrap(); // worker-done

    let mut scratch = Vec::new();
    master_side
        .channel()
        .response
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    master_side
        .channel()
        .response
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    assert!(scratch.is_empty());

    master_side.reclaim_if_due();
    assert_eq!(
        master_side
            .channel()
            .response
            .reclaimed_pos
            .load(Ordering::Relaxed),
        0
    );
}

/// The request ring fills on request *count*, not size - the positions wrap,
/// so small requests reach every page too. Asserted on the memfd's block
/// count, `reclaimed_pos` advancing being no proof a punch happened.
#[test]
fn reclaim_request_returns_pages_that_small_requests_accumulated() {
    let (fd, worker_side) = create_channel().unwrap();
    let master_side = map_existing_channel(fd.try_clone().unwrap()).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    let master_peer = &master_side.channel().peer_death;
    let worker_peer = &worker_side.channel().peer_death;

    let blocks_kb = || {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(fd.as_raw_fd(), &mut st) }, 0);
        (st.st_blocks as u64) * 512 / 1024
    };
    let before = blocks_kb();

    // Small enough that no single request comes close to the ring, and
    // enough of them to wrap it more than once.
    let request = vec![0x5Au8; 4 * 1024];
    let mut scratch = Vec::new();
    for _ in 0..200 {
        master_side
            .channel()
            .request
            .write_frame(&request, master_peer, efd_raw)
            .unwrap();
        worker_side
            .channel()
            .request
            .read_frame(&mut scratch, worker_peer, efd_raw)
            .unwrap();
    }
    let filled = blocks_kb();
    assert!(
        filled - before >= (REQUEST_RING_CAPACITY / 1024) as u64 - 16,
        "small requests should still have filled the ring, went from {before} to {filled} KB"
    );

    master_side.reclaim_if_due();
    let after = blocks_kb();
    assert!(
        after < filled / 2,
        "reclaim freed nothing: {filled} KB -> {after} KB"
    );

    // The ring must still work: master writes into the punched range and the
    // worker reads it back unchanged.
    master_side
        .channel()
        .request
        .write_frame(b"after the punch", master_peer, efd_raw)
        .unwrap();
    worker_side
        .channel()
        .request
        .read_frame(&mut scratch, worker_peer, efd_raw)
        .unwrap();
    assert_eq!(scratch, b"after the punch");
}

#[test]
fn reclaim_response_after_full_drain_then_reuses_the_space_correctly() {
    // The one safe calling pattern - see `reclaim_if_due`'s safety note.
    let (fd, prototype_side) = create_channel().unwrap();
    let master_side = map_existing_channel(fd).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    let proto_peer = &prototype_side.channel().peer_death;
    let master_peer = &master_side.channel().peer_death;

    const FRAME: [u8; 8192] = [0xAB; 8192];
    const N_FRAMES: usize = 10; // 80KB, comfortably over RECLAIM_THRESHOLD

    for _ in 0..N_FRAMES {
        prototype_side
            .channel()
            .response
            .write_frame(&FRAME, proto_peer, efd_raw)
            .unwrap();
    }
    prototype_side
        .channel()
        .response
        .write_frame(&[], proto_peer, efd_raw)
        .unwrap(); // worker-done

    let mut scratch = Vec::new();
    for _ in 0..N_FRAMES {
        master_side
            .channel()
            .response
            .read_frame(&mut scratch, master_peer, efd_raw)
            .unwrap();
        assert_eq!(scratch, FRAME);
    }
    master_side
        .channel()
        .response
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    assert!(scratch.is_empty());

    master_side.reclaim_if_due();
    assert!(
        master_side
            .channel()
            .response
            .reclaimed_pos
            .load(Ordering::Relaxed)
            > 0
    );

    // Reuses the exact space the punch touched.
    const FRAME2: [u8; 8192] = [0xCD; 8192];
    for _ in 0..N_FRAMES {
        prototype_side
            .channel()
            .response
            .write_frame(&FRAME2, proto_peer, efd_raw)
            .unwrap();
    }
    prototype_side
        .channel()
        .response
        .write_frame(&[], proto_peer, efd_raw)
        .unwrap();
    for _ in 0..N_FRAMES {
        master_side
            .channel()
            .response
            .read_frame(&mut scratch, master_peer, efd_raw)
            .unwrap();
        assert_eq!(scratch, FRAME2);
    }
    master_side
        .channel()
        .response
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    assert!(scratch.is_empty());
}

#[test]
fn reclaim_after_multiple_wraps_frees_the_whole_ring_not_just_the_final_lap() {
    // Past a full lap, masked positions see only the final lap's range while
    // `reclaimed_pos` still jumps to `read_pos`, losing the rest forever.
    // Asserted on the memfd's real block count, because the bug advanced the
    // bookkeeping correctly and simply did not punch.
    let (fd, prototype_side) = create_channel().unwrap();
    let stat_fd = nix::unistd::dup(&fd).unwrap();
    let master_side = map_existing_channel(fd).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    let proto_peer = &prototype_side.channel().peer_death;
    let master_peer = &master_side.channel().peer_death;

    let blocks = || nix::sys::stat::fstat(&stat_fd).unwrap().st_blocks;
    let baseline = blocks();

    const FRAME: [u8; 8192] = [0xAB; 8192];
    const FRAMES_PER_ROUND: usize = 11; // ~90KB/round incl. the 4-byte length prefix each
    const ROUNDS: usize = 4; // sums to well over CAPACITY (256KiB) - several full wraps

    let mut scratch = Vec::new();
    for _ in 0..ROUNDS {
        for _ in 0..FRAMES_PER_ROUND {
            prototype_side
                .channel()
                .response
                .write_frame(&FRAME, proto_peer, efd_raw)
                .unwrap();
        }
        prototype_side
            .channel()
            .response
            .write_frame(&[], proto_peer, efd_raw)
            .unwrap(); // worker-done
        for _ in 0..FRAMES_PER_ROUND {
            master_side
                .channel()
                .response
                .read_frame(&mut scratch, master_peer, efd_raw)
                .unwrap();
            assert_eq!(scratch, FRAME);
        }
        master_side
            .channel()
            .response
            .read_frame(&mut scratch, master_peer, efd_raw)
            .unwrap();
        assert!(scratch.is_empty());
        // Accumulate past a full lap before reclaiming at all.
    }

    let read_pos = master_side
        .channel()
        .response
        .read_pos
        .load(Ordering::Relaxed);
    assert!(
        read_pos > RESPONSE_RING_CAPACITY as u64,
        "test setup must span more than one full lap"
    );

    let after_writes = blocks();
    assert!(
        after_writes > baseline,
        "the writes above should have actually allocated pages"
    );

    master_side.reclaim_if_due();
    assert_eq!(
        master_side
            .channel()
            .response
            .reclaimed_pos
            .load(Ordering::Relaxed),
        read_pos
    );

    let after_reclaim = blocks();
    let allocated_by_writes = after_writes - baseline;
    let freed_by_reclaim = after_writes - after_reclaim;
    assert!(
        freed_by_reclaim >= allocated_by_writes * 8 / 10,
        "reclaim should free essentially everything the writes allocated \
         (baseline={baseline} after_writes={after_writes} after_reclaim={after_reclaim}) - \
         freeing only a sliver means the multi-wrap under-punch bug is back"
    );
}

/// The one invariant the whole reclaim rests on, checked rather than promised:
/// a ring with anything unread in it must not be punched, because the writer
/// may be filling exactly the range that would be freed.
///
/// A `debug_assert` would not do - it is compiled out of the very builds that
/// serve traffic, which is where zeroing live data would actually happen.
#[test]
fn reclaim_is_skipped_while_the_writer_still_has_data_in_flight() {
    let (fd, prototype_side) = create_channel().unwrap();
    let stat_fd = nix::unistd::dup(&fd).unwrap();
    let master_side = map_existing_channel(fd).unwrap();
    let efd = make_notify_efd();
    let efd_raw = efd.as_raw_fd();
    let proto_peer = &prototype_side.channel().peer_death;
    let master_peer = &master_side.channel().peer_death;
    let blocks = || nix::sys::stat::fstat(&stat_fd).unwrap().st_blocks;

    // Past RECLAIM_THRESHOLD, so only the writer-idle check can stop the punch.
    const FRAME: [u8; 8192] = [0xAB; 8192];
    let mut scratch = Vec::new();
    for _ in 0..10 {
        prototype_side
            .channel()
            .response
            .write_frame(&FRAME, proto_peer, efd_raw)
            .unwrap();
        master_side
            .channel()
            .response
            .read_frame(&mut scratch, master_peer, efd_raw)
            .unwrap();
    }

    // One frame the reader has not taken: the ring is no longer empty.
    prototype_side
        .channel()
        .response
        .write_frame(&FRAME, proto_peer, efd_raw)
        .unwrap();
    let before = blocks();
    master_side.reclaim_if_due();
    assert_eq!(
        blocks(),
        before,
        "a ring with data in flight was punched anyway"
    );

    // Draining it makes the same call proceed, so the skip was the check and
    // not some unrelated reason to do nothing.
    master_side
        .channel()
        .response
        .read_frame(&mut scratch, master_peer, efd_raw)
        .unwrap();
    master_side.reclaim_if_due();
    assert!(
        blocks() < before,
        "reclaim did not resume once the ring drained"
    );
}

// --- Master-side async wait path (`*_async`, eventfd-backed) ---

#[tokio::test]
async fn read_frame_async_returns_immediately_when_data_already_present() {
    // Data is already there, so this must resolve without the eventfd.
    // Awaiting a readiness nobody signals would hang rather than fail.
    let ring: &Ring<64> = make_ring();
    let peer = make_peer_death();
    let write_efd = make_notify_efd();
    ring.write_frame(b"already here", peer, write_efd.as_raw_fd())
        .unwrap();

    let read_efd = AsyncFd::new(make_notify_efd()).unwrap();
    let mut scratch = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        ring.read_frame_async(&mut scratch, peer, &read_efd),
    )
    .await
    .expect("should not need to wait")
    .unwrap();
    assert_eq!(scratch, b"already here");
}

#[tokio::test]
async fn write_frame_async_then_blocking_read_frame_roundtrip() {
    // The real `RequestRing` direction.
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let space_efd = AsyncFd::new(make_notify_efd()).unwrap();
    // Unused here; just needs to be a real fd.
    let data_efd = make_notify_efd();

    ring.write_frame_async(b"hello async", peer, &space_efd)
        .await
        .unwrap();

    let data_efd_raw = data_efd.as_raw_fd();
    let handle = std::thread::spawn(move || {
        let mut scratch = Vec::new();
        ring.read_frame(&mut scratch, peer, data_efd_raw).unwrap();
        scratch
    });
    let scratch = tokio::task::spawn_blocking(move || handle.join().unwrap())
        .await
        .unwrap();
    assert_eq!(scratch, b"hello async");
}

#[tokio::test]
async fn blocking_write_frame_wakes_a_parked_read_frame_async() {
    // The reader parks before any data exists, so only a real cross-thread
    // eventfd wake can release it. Broken plumbing hangs here rather than
    // failing an assertion.
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let data_efd_owned = make_notify_efd();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();
    let space_efd = make_notify_efd(); // worker's own notify target, unused here

    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        ring.write_frame(b"woke you up", peer, data_efd_raw)
            .unwrap();
    });

    let mut scratch = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        ring.read_frame_async(&mut scratch, peer, &data_efd),
    )
    .await
    .expect("blocking writer's eventfd_write should have woken this before the timeout")
    .unwrap();
    assert_eq!(scratch, b"woke you up");

    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();
    drop(space_efd);
}

#[tokio::test]
async fn peer_death_wakes_a_parked_wait_for_data_async() {
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let data_efd_owned = make_notify_efd();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();

    let killer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        peer.mark_dead();
        // `mark_peer_dead` reaches only futex waiters, so the eventfd needs
        // its own wake, as the liveness watcher does.
        eventfd_notify(data_efd_raw);
    });

    let mut scratch = Vec::new();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ring.read_frame_async(&mut scratch, peer, &data_efd),
    )
    .await
    .expect("peer-death eventfd_notify should have woken this before the timeout");
    assert!(matches!(result, Err(RingError::PeerGone)));

    tokio::task::spawn_blocking(move || killer.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_blocking_write_and_master_async_read_survive_real_contention() {
    // A small ring and many frames force both sides to park repeatedly
    // rather than resolving on the fast path.
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let data_efd_owned = make_notify_efd();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();
    const N: usize = 2_000;

    let writer = std::thread::spawn(move || {
        for i in 0..N as u32 {
            ring.write_frame(&i.to_le_bytes(), peer, data_efd_raw)
                .unwrap();
        }
    });

    let mut scratch = Vec::new();
    for i in 0..N as u32 {
        tokio::time::timeout(
            Duration::from_secs(10),
            ring.read_frame_async(&mut scratch, peer, &data_efd),
        )
        .await
        .expect("should not hang - see this test's own doc comment")
        .unwrap();
        assert_eq!(scratch.as_slice(), &i.to_le_bytes());
    }

    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn master_async_write_and_worker_blocking_read_survive_real_contention() {
    // The sibling pairing, on the request ring.
    let ring: &'static Ring<64> = make_ring();
    let peer: &'static PeerDeath = make_peer_death();
    let space_efd_owned = make_notify_efd();
    let space_efd_raw = space_efd_owned.as_raw_fd();
    let space_efd = AsyncFd::new(space_efd_owned).unwrap();
    const N: usize = 2_000;

    let reader = std::thread::spawn(move || {
        let mut scratch = Vec::new();
        for i in 0..N as u32 {
            ring.read_frame(&mut scratch, peer, space_efd_raw).unwrap();
            assert_eq!(scratch.as_slice(), &i.to_le_bytes());
        }
    });

    for i in 0..N as u32 {
        tokio::time::timeout(
            Duration::from_secs(10),
            ring.write_frame_async(&i.to_le_bytes(), peer, &space_efd),
        )
        .await
        .expect("should not hang - see the sibling test's own doc comment")
        .unwrap();
    }

    tokio::task::spawn_blocking(move || reader.join().unwrap())
        .await
        .unwrap();
}

/// `MappedChannel`'s `Send` and `Sync` are hand-written. Every other
/// concurrency test here shares a bare `Ring`, so nothing exercises the claim
/// that the mapping itself may cross threads and be used from several at once.
///
/// Moving one mapping into a thread covers `Send`; reaching the other from two
/// at the same time covers `Sync`.
#[tokio::test]
async fn a_mapped_channel_survives_being_shared_across_threads() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    const N: usize = 2_000;

    let (fd, worker_side) = create_channel().unwrap();
    let master_side = Arc::new(map_existing_channel(fd).unwrap());
    let data_efd_owned = make_notify_efd();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();

    // Send: the whole mapping moves to the writer, exactly as the prototype
    // hands its own to a forked worker.
    let writer = std::thread::spawn(move || {
        let channel = worker_side.channel();
        for i in 0..N as u32 {
            channel
                .response
                .write_frame(&i.to_le_bytes(), &channel.peer_death, data_efd_raw)
                .unwrap();
        }
    });

    // Sync: a second thread holds `&MappedChannel` while the reader below uses
    // it too, so both are dereferencing the same mapping concurrently.
    let stop = Arc::new(AtomicBool::new(false));
    let observer = std::thread::spawn({
        let (mapped, stop) = (Arc::clone(&master_side), Arc::clone(&stop));
        move || {
            let mut seen = 0u64;
            while !stop.load(Ordering::Relaxed) {
                seen = seen.max(mapped.channel().response.write_pos.load(Ordering::Acquire));
            }
            seen
        }
    });

    let mut scratch = Vec::new();
    for i in 0..N as u32 {
        let channel = master_side.channel();
        tokio::time::timeout(
            Duration::from_secs(10),
            channel
                .response
                .read_frame_async(&mut scratch, &channel.peer_death, &data_efd),
        )
        .await
        .expect("should not hang - the real pairing, see this module's own note")
        .unwrap();
        assert_eq!(
            scratch.as_slice(),
            &i.to_le_bytes(),
            "frame {i} came back wrong"
        );
    }
    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();

    stop.store(true, Ordering::Relaxed);
    let seen = observer.join().unwrap();
    assert!(
        seen > 0,
        "the observer never saw the mapping advance - it was not really sharing it"
    );
}

// --- `NotifyEfds::try_clone` (fd-lifetime independence) ---

#[test]
fn try_clone_gives_a_different_fd_number_for_the_same_underlying_eventfd() {
    let notify = NotifyEfds {
        req_space: make_notify_efd(),
        resp_data: make_notify_efd(),
    };
    let cloned = notify.try_clone().unwrap();

    assert_ne!(notify.req_space.as_raw_fd(), cloned.req_space.as_raw_fd());

    // Same underlying eventfd.
    eventfd_notify(cloned.req_space.as_raw_fd());
    let mut buf = [0u8; 8];
    let n = unsafe {
        libc::read(
            notify.req_space.as_raw_fd(),
            buf.as_mut_ptr() as *mut libc::c_void,
            8,
        )
    };
    assert_eq!(n, 8);
    assert_eq!(u64::from_ne_bytes(buf), 1);
}

#[test]
fn try_clone_survives_the_original_being_dropped() {
    // Closing the original must neither invalidate the clone nor let the
    // reused fd number alias it.
    let notify = NotifyEfds {
        req_space: make_notify_efd(),
        resp_data: make_notify_efd(),
    };
    let cloned = notify.try_clone().unwrap();
    let cloned_raw = cloned.req_space.as_raw_fd();

    drop(notify);
    // Make the reuse concrete rather than theoretical.
    let _decoys: Vec<_> = (0..4).map(|_| make_notify_efd()).collect();

    eventfd_notify(cloned_raw);
    let mut buf = [0u8; 8];
    let n = unsafe { libc::read(cloned_raw, buf.as_mut_ptr() as *mut libc::c_void, 8) };
    assert_eq!(n, 8);
    assert_eq!(u64::from_ne_bytes(buf), 1);
}

// --- park protocol: the two lost-wakeup windows ---

/// The contract the park protocol rests on: publish a claim, recheck under
/// it, and retract it on every path that does not end in a park. Leaving it
/// published wastes a wake; retracting it before parking loses one.
#[test]
fn declare_waiting_retracts_the_claim_unless_the_caller_will_actually_park() {
    let peer = make_peer_death();
    let state = AtomicU32::new(EMPTY);

    let park = Ring::<64>::declare_waiting(&state, peer, || true).unwrap();
    assert!(!park, "no park is needed once the condition holds");
    assert_eq!(
        state.load(Ordering::SeqCst),
        EMPTY,
        "claim must be retracted, not left set"
    );

    // The claim must stay published for `take_waiter` to see.
    let park = Ring::<64>::declare_waiting(&state, peer, || false).unwrap();
    assert!(park);
    assert_eq!(
        state.load(Ordering::SeqCst),
        WAITING,
        "the claim is what makes the notify fire at all"
    );

    peer.mark_dead();
    let err = Ring::<64>::declare_waiting(&state, peer, || false).unwrap_err();
    assert!(matches!(err, RingError::PeerGone));
    assert_eq!(state.load(Ordering::SeqCst), EMPTY);
}

/// The second lost-wakeup window, made deterministic.
///
/// A waiter descheduled between publishing `WAITING` and entering
/// `futex_wait` cannot be reached by a wake: it is not parked yet, and by
/// the time it parks the wake is gone. Stamping `DEAD` over the word turns
/// that sleep into an immediate `EAGAIN`.
///
/// Asserts the stamp directly rather than trying to hit the race; the word's
/// `WAITING` stands in for the descheduled waiter.
#[test]
fn mark_peer_dead_stamps_over_a_published_claim_so_futex_wait_cannot_sleep() {
    let (_fd, mapped) = create_channel().unwrap();
    let channel = mapped.channel();

    // The state a waiter leaves behind on the brink of parking.
    channel.request.data_state.store(WAITING, Ordering::SeqCst);
    channel
        .response
        .space_state
        .store(WAITING, Ordering::SeqCst);

    channel.mark_peer_dead();

    assert!(
        channel.peer_death.is_dead(),
        "peer_death stays the authority"
    );
    for (name, word) in [
        ("request.data_state", &channel.request.data_state),
        ("request.space_state", &channel.request.space_state),
        ("response.data_state", &channel.response.data_state),
        ("response.space_state", &channel.response.space_state),
    ] {
        assert_eq!(
            word.load(Ordering::SeqCst),
            DEAD,
            "{name} still reads WAITING - a waiter about to park there would sleep through the only wake it gets"
        );
    }
}

/// The stamp only helps if the bounced waiter then leaves: a retry that
/// re-publishes `WAITING` and parks again burns the wake for nothing, so the
/// exit has to come from `peer_death` being set alongside it.
#[test]
fn a_waiter_bounced_by_the_dead_stamp_finds_peer_death_already_set() {
    let (_fd, mapped) = create_channel().unwrap();
    let channel = mapped.channel();
    channel.mark_peer_dead();

    // Stands in for the retry loop after an EAGAIN.
    let err = Ring::<REQUEST_RING_CAPACITY>::declare_waiting(
        &channel.request.data_state,
        &channel.peer_death,
        || false,
    )
    .unwrap_err();
    assert!(
        matches!(err, RingError::PeerGone),
        "the retry must exit, not re-park"
    );
}

/// `take_waiter` may clobber `DEAD` back to `EMPTY`; this pins that it stays
/// harmless, a later waiter still exiting via `peer_death`.
#[test]
fn a_notify_landing_after_peer_death_does_not_resurrect_the_ring() {
    let (_fd, mapped) = create_channel().unwrap();
    let channel = mapped.channel();
    channel.mark_peer_dead();

    Ring::<REQUEST_RING_CAPACITY>::take_waiter(&channel.request.data_state);
    assert_eq!(channel.request.data_state.load(Ordering::SeqCst), EMPTY);

    let mut scratch = Vec::new();
    let efd = make_notify_efd();
    let err = channel
        .request
        .read_frame(&mut scratch, &channel.peer_death, efd.as_raw_fd())
        .unwrap_err();
    assert!(
        matches!(err, RingError::PeerGone),
        "peer_death must still be the authority after the word was cleared"
    );
}

/// A tiny ring against tiny frames, so nearly every frame round-trips
/// through a real park and wake in both pairings. Dropped wakes show up as a
/// hang.
///
/// Does NOT cover the StoreLoad window `declare_waiting` closes: it is
/// reachable only on x86_64 and too narrow to hit reliably even there, so
/// reverting that ordering leaves this test green. A liveness smoke test,
/// not a memory-model check.
#[tokio::test]
async fn tight_park_wake_handoff_never_loses_a_wakeup() {
    const N: u32 = 20_000;

    {
        let ring: &'static Ring<16> = make_ring();
        let peer: &'static PeerDeath = make_peer_death();
        let space_efd_owned = make_notify_efd();
        let space_efd_raw = space_efd_owned.as_raw_fd();
        let space_efd = AsyncFd::new(space_efd_owned).unwrap();

        let reader = std::thread::spawn(move || {
            let mut scratch = Vec::new();
            for i in 0..N {
                ring.read_frame(&mut scratch, peer, space_efd_raw).unwrap();
                assert_eq!(scratch.as_slice(), &i.to_le_bytes());
            }
        });
        for i in 0..N {
            tokio::time::timeout(
                Duration::from_secs(10),
                ring.write_frame_async(&i.to_le_bytes(), peer, &space_efd),
            )
            .await
            .expect("hung waiting for space - a wakeup was lost")
            .unwrap();
        }
        tokio::task::spawn_blocking(move || reader.join().unwrap())
            .await
            .unwrap();
    }

    {
        let ring: &'static Ring<16> = make_ring();
        let peer: &'static PeerDeath = make_peer_death();
        let data_efd_owned = make_notify_efd();
        let data_efd_raw = data_efd_owned.as_raw_fd();
        let data_efd = AsyncFd::new(data_efd_owned).unwrap();

        let writer = std::thread::spawn(move || {
            for i in 0..N {
                ring.write_frame(&i.to_le_bytes(), peer, data_efd_raw)
                    .unwrap();
            }
        });
        let mut scratch = Vec::new();
        for i in 0..N {
            tokio::time::timeout(
                Duration::from_secs(10),
                ring.read_frame_async(&mut scratch, peer, &data_efd),
            )
            .await
            .expect("hung waiting for data - a wakeup was lost")
            .unwrap();
            assert_eq!(scratch.as_slice(), &i.to_le_bytes());
        }
        tokio::task::spawn_blocking(move || writer.join().unwrap())
            .await
            .unwrap();
    }
}
