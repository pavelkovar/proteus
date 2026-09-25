use super::*;
use nix::sys::signal::kill;
use nix::sys::wait::{WaitStatus, waitpid};

/// How many workers the fixture pool admits; the lifecycle tests lean on the
/// seat coming back, so they need room for more than one at a time.
const TEST_POOL_MAX: usize = 4;

/// Enough real state for the lifecycle tests, which touch only the prototype
/// handle, the admission seats and the counters. `prototype_pid` must name a
/// real killable process, since these tests signal and reap it.
pub(super) fn make_test_pool_manager(prototype_pid: u32) -> PoolManager {
    make_test_pool_manager_with_control(prototype_pid).0
}

/// Also returns the prototype's end of the control socket.
pub(super) fn make_test_pool_manager_with_control(
    prototype_pid: u32,
) -> (PoolManager, UnixSeqpacket) {
    let (control_sock, prototype_end) = UnixSeqpacket::pair().unwrap();
    let pool = PoolManager {
        kill_channel: StdMutex::new(dup_control(&control_sock)),
        prototype_generation: AtomicU64::new(0),
        control: Mutex::new(control_sock),
        control_rt: tokio::runtime::Handle::current(),
        idle: StdMutex::new(VecDeque::new()),
        worker_returned: Notify::new(),
        maintenance: Arc::new(Notify::new()),
        active: AtomicBool::new(true),
        semaphore: Arc::new(Semaphore::new(1)),
        admission: Arc::new(Semaphore::new(TEST_POOL_MAX)),
        max_workers: 1,
        request_timeout: Some(Duration::from_secs(30)),
        idle_timeout: None,
        queue_timeout: Duration::from_secs(5),
        spawn_timeout: Duration::from_secs(30),
        queue_max_depth: 0,
        queue_depth: Gauge::default(),
        started_at: Instant::now(),
        target_names: Vec::new(),
        counters: Counters::default(),
        prototype_child: StdMutex::new(Handle::new(prototype_pid)),
        prototype_spec: prototype::Spec {
            config: ProtoConfig::default(),
            drop_to: None,
            no_new_privs: true,
        },
        respawn_backoff: Mutex::new(RespawnBackoff::default()),
        workers: StdMutex::new(HashMap::new()),
    };
    (pool, prototype_end)
}

async fn kills_sent(prototype_end: &UnixSeqpacket) -> Vec<u32> {
    let mut pids = Vec::new();
    let mut buf = [0u8; 32];
    while let Ok(Ok(n)) =
        tokio::time::timeout(Duration::from_millis(100), prototype_end.recv(&mut buf)).await
    {
        if let control::Command::Kill(pid) = control::parse_command(&buf[..n.bytes_read()]) {
            pids.push(pid);
        }
    }
    pids
}

/// A real, killable process standing in for a prototype or a worker; which
/// one it is does not matter.
// The `Child` is dropped unwaited on purpose: these tests reap through
// `waitpid`, as master does, and a second reaper would race it.
#[allow(clippy::zombie_processes)]
pub(super) fn spawn_sleeper() -> u32 {
    let child = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .expect("failed to spawn `sleep 100`");
    child.id()
}

#[test]
fn respawn_backoff_delay_grows_and_caps() {
    assert_eq!(respawn_backoff_delay(0), Duration::from_secs(1));
    assert_eq!(respawn_backoff_delay(1), Duration::from_secs(2));
    assert_eq!(respawn_backoff_delay(3), Duration::from_secs(8));
    assert_eq!(
        respawn_backoff_delay(6),
        Duration::from_secs(60),
        "caps at 64s -> 60s cap"
    );
    assert_eq!(
        respawn_backoff_delay(20),
        Duration::from_secs(60),
        "stays capped for large inputs"
    );
}

/// That a real process actually dies, not merely that `kill()` did not error.
#[test]
fn sigkill_actually_terminates_the_process() {
    let mut child = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .expect("failed to spawn `sleep 100` for this test");
    let pid = child.id();

    sigkill(pid, "test");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(Some(_)) = child.try_wait() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "process pid={pid} was not terminated by sigkill()"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Just enough to identify a request in `/status`; nothing reads it back
/// over the wire.
pub(super) fn dummy_request() -> Arc<crate::ipc::data::PhpRequest<'static>> {
    use std::borrow::Cow;
    Arc::new(crate::ipc::data::PhpRequest {
        script_path: Cow::Borrowed("/var/www/app/index.php"),
        document_root: Cow::Borrowed("/var/www/app"),
        script_name: Cow::Borrowed("/index.php"),
        path_info: Cow::Borrowed(""),
        method: Cow::Borrowed("GET"),
        uri: Cow::Borrowed("/hello"),
        headers: crate::ipc::data::HeaderBlob::default(),
        client_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        body: crate::ipc::data::RequestBody::Inline(Cow::Borrowed(&[])),
        server_name: Cow::Borrowed("localhost"),
        server_addr: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        server_port: 80,
        server_protocol: Cow::Borrowed("HTTP/1.1"),
        https: false,
    })
}

/// The busy/idle bookkeeping runs with no pool-wide lock and no pid lookup;
/// this pins that what `status_json` reports is still exact.
#[test]
fn worker_meta_tracks_state_and_request_count_without_the_pool_lock() {
    let pool_started = Instant::now();
    let meta = detached_meta(pool_started);
    assert_eq!(meta.state_str(), "idle");
    assert_eq!(meta.request_count.load(Relaxed), 0);

    meta.mark_busy(Instant::now(), pool_started, dummy_request());
    assert_eq!(meta.state_str(), "busy");
    assert_eq!(
        meta.request_count.load(Relaxed),
        1,
        "each dispatch counts exactly once"
    );

    meta.mark_idle(Instant::now(), pool_started);
    assert_eq!(meta.state_str(), "idle");
    assert_eq!(
        meta.request_count.load(Relaxed),
        1,
        "returning to idle must not count as another request"
    );

    meta.mark_busy(Instant::now(), pool_started, dummy_request());
    assert_eq!(meta.request_count.load(Relaxed), 2);
}

#[test]
fn current_request_json_names_the_in_flight_request_and_clears_on_idle() {
    let pool_started = Instant::now();
    let meta = detached_meta(pool_started);
    assert!(meta.current_request_json().is_null());

    meta.mark_busy(Instant::now(), pool_started, dummy_request());
    assert_eq!(meta.current_request_json()["method"], "GET");
    assert_eq!(meta.current_request_json()["uri"], "/hello");
    assert_eq!(
        meta.current_request_json()["script"],
        "/var/www/app/index.php"
    );

    meta.mark_idle(Instant::now(), pool_started);
    assert!(meta.current_request_json().is_null());
}

/// Concurrent dispatches on distinct workers must need no lock between them
/// and still count exactly.
#[test]
fn worker_meta_counts_are_exact_under_concurrent_updates() {
    let pool_started = Instant::now();
    let meta = Arc::new(detached_meta(pool_started));
    const THREADS: usize = 8;
    const PER_THREAD: usize = 1000;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let meta = Arc::clone(&meta);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    meta.mark_busy(Instant::now(), pool_started, dummy_request());
                    meta.mark_idle(Instant::now(), pool_started);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        meta.request_count.load(Relaxed),
        (THREADS * PER_THREAD) as u32
    );
    assert_eq!(
        meta.state_str(),
        "idle",
        "the last write of every thread was mark_idle"
    );
}

/// A respawn may replace a prototype that is still running but no longer
/// answering. Overwriting the handle without killing it would orphan it:
/// alive, holding its PHP heap, referenced and reaped by nobody.
#[tokio::test]
async fn replacing_a_still_running_prototype_kills_and_reaps_it() {
    let old_pid = spawn_sleeper() as i32;
    let pool = make_test_pool_manager(old_pid as u32);

    let new_pid = spawn_sleeper();
    pool.prototype_child.lock().unwrap().replace(new_pid);

    // Signal 0 probes existence without sending anything. Already reaped, so
    // ESRCH rather than a zombie still answering.
    let alive = kill(Pid::from_raw(old_pid), None).is_ok();
    assert!(
        !alive,
        "the replaced prototype pid={old_pid} is still around - it was orphaned, not killed and reaped"
    );
    assert_eq!(
        pool.prototype_child.lock().unwrap().pid(),
        new_pid,
        "the replacement must be the tracked one"
    );
    reap_tracked_prototype(&pool);
}

pub(super) fn reap_tracked_prototype(pool: &PoolManager) {
    let pid = Pid::from_raw(pool.prototype_child.lock().unwrap().pid() as i32);
    let _ = kill(pid, Signal::SIGKILL);
    let _ = waitpid(pid, None);
}

/// Process state from `/proc/[pid]/stat`, located from the last `)` since
/// `comm` is parenthesized and may contain spaces. Signal 0 will not do: it
/// succeeds on a zombie, so it cannot tell a survivor from a fresh corpse.
fn proc_state(pid: i32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .next()?
        .chars()
        .next()
}

/// A reaped pid may already belong to something else, so it must never be
/// signalled again. Simulated by pointing a reaped handle at a live process,
/// which stands in for whatever the kernel handed the number to next.
#[tokio::test]
async fn a_reaped_prototype_pid_is_never_signalled_again() {
    let squatter_pid = spawn_sleeper() as i32;
    let pool = make_test_pool_manager(squatter_pid as u32);
    {
        let mut handle = pool.prototype_child.lock().unwrap();
        handle.note_exit(
            squatter_pid as u32,
            WaitStatus::Exited(Pid::from_raw(squatter_pid), 0),
        );
        handle.kill("a reaped pid must never be signalled");
    }

    // Waits out delivery rather than assuming it: a signal that was sent shows
    // up as a zombie, since nothing here reaps.
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut state = proc_state(squatter_pid);
    while Instant::now() < deadline && state != Some('Z') {
        tokio::time::sleep(Duration::from_millis(20)).await;
        state = proc_state(squatter_pid);
    }

    let pid = Pid::from_raw(squatter_pid);
    let _ = kill(pid, Signal::SIGKILL);
    let _ = waitpid(pid, None);
    assert_ne!(
        state,
        Some('Z'),
        "a reaped pid was signalled - a recycled number would have taken the hit"
    );
}

/// The already-dead case must stay a plain reap: no signal to a pid the OS
/// may have handed to something else.
#[tokio::test]
async fn replacing_an_already_exited_prototype_just_reaps_it() {
    let mut old = std::process::Command::new("true").spawn().unwrap();
    let old_pid = old.id() as i32;
    // Exit and be reaped first, so this takes the ECHILD branch rather than
    // racing it.
    while !matches!(old.try_wait(), Ok(Some(_))) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let pool = make_test_pool_manager(old_pid as u32);

    pool.prototype_child
        .lock()
        .unwrap()
        .replace(spawn_sleeper());

    assert!(
        kill(Pid::from_raw(old_pid), None).is_err(),
        "an exited prototype must not linger"
    );

    reap_tracked_prototype(&pool);
}

fn unused_link() -> std::os::fd::OwnedFd {
    let (a, _b) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::SeqPacket,
        None,
        nix::sys::socket::SockFlag::empty(),
    )
    .expect("socketpair");
    a
}

fn idle_worker(pool: &PoolManager, pid: u32, retired: bool) -> PooledWorker {
    let (fd, _worker_side) = crate::ipc::shm::create_channel().unwrap();
    let mapped = crate::ipc::shm::map_existing_channel(fd).unwrap();
    let channel = crate::master::pool_manager::worker_channel::WorkerChannel::for_test(
        pid,
        Arc::new(mapped),
        crate::ipc::shm::NotifyEfds {
            req_space: crate::ipc::shm::create_notify_eventfd().unwrap(),
            resp_data: crate::ipc::shm::create_notify_eventfd().unwrap(),
        },
        unused_link(),
    );
    if retired {
        channel.mark_worker_gone_for_test();
    }
    PooledWorker {
        channel,
        pid,
        id: WorkerId::next(),
        meta: Arc::new(seated_meta(pool, pid)),
    }
}

/// A `WorkerMeta` holding one of `pool`'s admission seats, as a real spawn
/// gives it.
fn seated_meta(pool: &PoolManager, pid: u32) -> WorkerMeta {
    WorkerMeta::new(
        Arc::clone(&pool.admission)
            .try_acquire_owned()
            .expect("the fixture pool has a free seat"),
        pool.started_at,
        pool.started_at,
        pid,
    )
}

/// For the tests that exercise `WorkerMeta` on its own, with no pool behind it.
fn detached_meta(pool_started: Instant) -> WorkerMeta {
    let seats = Arc::new(Semaphore::new(1));
    WorkerMeta::new(
        seats.try_acquire_owned().expect("a fresh semaphore"),
        pool_started,
        pool_started,
        0,
    )
}

/// A worker found already gone (crash, external kill) must be found wherever
/// it is parked. Dispatch only ever sees the back, so anything the sweep
/// fails to rotate past is reported idle forever.
#[tokio::test]
async fn a_vanished_worker_is_reaped_from_any_position() {
    for position in 0..3 {
        let pool = make_test_pool_manager(spawn_sleeper());
        for (offset, pid) in [101u32, 102, 103].into_iter().enumerate() {
            let worker = idle_worker(&pool, pid, offset == position);
            pool.idle.lock().unwrap().push_back(worker);
        }

        pool.sweep_idle_workers(3).await;

        let left: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
        assert_eq!(
            left.len(),
            2,
            "the vanished worker at position {position} was never looked at"
        );
        assert!(!left.contains(&(101 + position as u32)));
        assert_eq!(pool.counters.workers_vanished_idle.load(Relaxed), 1);
        reap_tracked_prototype(&pool);
    }
}

/// A full rotation has to leave the survivors as it found them, or every
/// sweep would reshuffle which worker dispatch reaches first.
#[tokio::test]
async fn a_sweep_leaves_the_order_it_found() {
    let pool = make_test_pool_manager(spawn_sleeper());
    for pid in [201u32, 202, 203, 204] {
        let worker = idle_worker(&pool, pid, false);
        pool.idle.lock().unwrap().push_back(worker);
    }

    pool.sweep_idle_workers(4).await;

    let after: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
    assert_eq!(after, vec![201, 202, 203, 204]);
    reap_tracked_prototype(&pool);
}

/// Only the coldest workers beyond `spare` are eligible for idle timeout; the
/// warmest `spare` of them must survive no matter how long they have sat idle.
///
/// Real spawned processes, not placeholder pids: `IdleTimeout` now signals
/// the worker, and a placeholder pid could collide with an unrelated real one.
#[tokio::test]
async fn idle_timeout_never_touches_the_spare_floor() {
    let (pool, prototype_end) = make_test_pool_manager_with_control(spawn_sleeper());
    let pool = PoolManager {
        idle_timeout: Some(Duration::from_secs(60)),
        // `idle_worker` stamps `last_active_ms` as "idle since pool start",
        // so backdating pool start is what makes every worker read as long idle.
        started_at: Instant::now() - Duration::from_secs(3600),
        ..pool
    };
    let pids: Vec<u32> = (0..4).map(|_| spawn_sleeper()).collect();
    for &pid in &pids {
        pool.idle
            .lock()
            .unwrap()
            .push_back(idle_worker(&pool, pid, false));
    }

    // 4 idle, spare 2: the coldest 2 (front) are excess and over the
    // timeout, the warmest 2 (back) are the protected floor.
    pool.sweep_idle_workers(2).await;

    let left: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
    assert_eq!(left, pids[2..].to_vec(), "only the floor should survive");
    assert_eq!(pool.counters.recycled_idle_timeout.load(Relaxed), 2);
    assert_eq!(
        kills_sent(&prototype_end).await,
        pids[..2].to_vec(),
        "exactly the excess idle workers should have been killed"
    );
    for &pid in &pids {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        let _ = waitpid(Pid::from_raw(pid as i32), None);
    }
    reap_tracked_prototype(&pool);
}

/// `idle_timeout` disabled (`None`, the default) must never evict anyone,
/// however long they have sat idle and however far over `spare`. Nothing gets
/// killed on this path, so placeholder pids are safe here.
#[tokio::test]
async fn disabled_idle_timeout_never_evicts_excess_workers() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let pool = PoolManager {
        started_at: Instant::now() - Duration::from_secs(3600),
        ..pool
    };
    for pid in [401u32, 402, 403] {
        let worker = idle_worker(&pool, pid, false);
        pool.idle.lock().unwrap().push_back(worker);
    }

    pool.sweep_idle_workers(1).await;

    let left: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
    assert_eq!(left, vec![401, 402, 403]);
    assert_eq!(pool.counters.recycled_idle_timeout.load(Relaxed), 0);
    reap_tracked_prototype(&pool);
}

/// Exactly at the floor - no excess at all - must evict nobody, however long
/// they have sat idle. Guards the `>=` boundary in the live floor check.
#[tokio::test]
async fn idle_timeout_evicts_nobody_when_idle_count_equals_spare() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let pool = PoolManager {
        idle_timeout: Some(Duration::from_secs(60)),
        started_at: Instant::now() - Duration::from_secs(3600),
        ..pool
    };
    for pid in [501u32, 502] {
        let worker = idle_worker(&pool, pid, false);
        pool.idle.lock().unwrap().push_back(worker);
    }

    pool.sweep_idle_workers(2).await;

    let left: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
    assert_eq!(left, vec![501, 502]);
    assert_eq!(pool.counters.recycled_idle_timeout.load(Relaxed), 0);
    reap_tracked_prototype(&pool);
}

/// A crash loop must not turn into a fork() storm: a respawn attempt inside
/// its own backoff window has to bail before ever touching `prototype::spawn`,
/// not just eventually fail once it gets there.
#[tokio::test]
async fn try_respawn_prototype_backs_off_within_its_own_window() {
    let pool = make_test_pool_manager(spawn_sleeper());
    {
        let mut backoff = pool.respawn_backoff.lock().await;
        // Just attempted, 0 prior failures -> a 1s window that this test's
        // own execution can't outlast.
        backoff.last_attempt = Some(Instant::now());
        backoff.consecutive_failures = 0;
    }

    let respawned = pool
        .try_respawn_prototype(pool.prototype_generation())
        .await;

    assert!(
        !respawned,
        "a respawn attempt within the backoff window must be refused"
    );
    assert_eq!(pool.counters.crash_loop_backoffs.load(Relaxed), 1);
    assert_eq!(
        pool.counters.prototype_respawns.load(Relaxed),
        0,
        "the backoff gate must fire before any real spawn is attempted"
    );
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn a_respawn_someone_else_already_did_is_reused_inside_the_backoff_window() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let observed = pool.prototype_generation();
    {
        let mut backoff = pool.respawn_backoff.lock().await;
        backoff.last_attempt = Some(Instant::now());
        backoff.consecutive_failures = 0;
    }
    pool.prototype_generation.fetch_add(1, Release);

    assert!(pool.try_respawn_prototype(observed).await);
    assert_eq!(pool.counters.crash_loop_backoffs.load(Relaxed), 0);
    assert_eq!(pool.counters.prototype_respawns.load(Relaxed), 0);
    reap_tracked_prototype(&pool);
}

/// Blocks until `pid` is a zombie, or gives up. Nothing in these tests reaps,
/// so a delivered SIGKILL leaves the corpse visible.
async fn became_a_zombie(pid: i32) -> bool {
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        if proc_state(pid) == Some('Z') {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// A live worker the pool accounts for, holding a seat as a real one does.
fn tracked_worker(pool: &PoolManager, pid: u32) -> WorkerId {
    let id = WorkerId::next();
    pool.track_worker(id, Arc::new(seated_meta(pool, pid)));
    id
}

/// Every retirement path gives the seat back. Leaking one lowers the pool's
/// real ceiling for good, and the reason it left must not change that.
#[tokio::test]
async fn every_retirement_reason_returns_the_seat() {
    let pool = make_test_pool_manager(spawn_sleeper());

    for why in [
        Retired::IdleTimeout,
        Retired::RequestLimit,
        Retired::Abandoned,
        Retired::Watchdog,
        Retired::Failed,
        Retired::Vanished,
        Retired::Unavailable,
    ] {
        // Out of range, so the kill variants fail harmlessly with ESRCH.
        const NO_REAL_WORKER_PID: u32 = 999_999_999;
        let id = tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(
            WorkerRef {
                id,
                pid: NO_REAL_WORKER_PID,
            },
            why,
        );
        assert_eq!(
            pool.admission.available_permits(),
            TEST_POOL_MAX,
            "retiring for {:?} kept the seat",
            why.as_str()
        );
        assert!(
            !pool.workers.lock().unwrap().contains_key(&id),
            "a retired worker is still tracked after {:?}",
            why.as_str()
        );
    }
    reap_tracked_prototype(&pool);
}

/// A worker that already exited on its own (`RequestLimit`, `Abandoned`,
/// `Vanished`) needs no signal - it's already gone. One still running,
/// including an `IdleTimeout` worker master decided to evict, must be killed.
#[tokio::test]
async fn only_the_reasons_that_leave_a_worker_running_kill_it() {
    let (pool, prototype_end) = make_test_pool_manager_with_control(spawn_sleeper());

    // Only ever sent to the prototype, so placeholder pids are safe.
    for (i, (why, expect_killed)) in [
        (Retired::IdleTimeout, true),
        (Retired::RequestLimit, false),
        (Retired::Abandoned, false),
        (Retired::Watchdog, true),
        (Retired::Failed, true),
        (Retired::Vanished, false),
        (Retired::Unavailable, true),
    ]
    .into_iter()
    .enumerate()
    {
        let worker_pid = 900_000 + i as u32;
        let id = tracked_worker(&pool, worker_pid);

        pool.retire(
            WorkerRef {
                id,
                pid: worker_pid,
            },
            why,
        );

        let expected = if expect_killed {
            vec![worker_pid]
        } else {
            vec![]
        };
        assert_eq!(
            kills_sent(&prototype_end).await,
            expected,
            "wrong kill decision for {:?}",
            why.as_str()
        );
    }
    reap_tracked_prototype(&pool);
}

/// Dropping the guard that owns a worker mid-dispatch (a client disconnect
/// before `Headers`) must free its seat and signal `peer_death`
/// synchronously - the safety net must never be what a fast worker waits on.
#[tokio::test]
async fn dropping_a_checked_out_worker_retires_and_signals_immediately() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));
    const NO_REAL_WORKER_PID: u32 = 999_999_993;
    let (worker, worker_side, _efd) = retiring_worker_fixture(&pool, NO_REAL_WORKER_PID);
    let id = worker.id;

    drop(super::dispatch::CheckedOutWorker::new(
        Arc::clone(&pool),
        worker,
    ));

    assert!(
        worker_side.channel().peer_death.is_dead(),
        "peer_death must be set synchronously, not deferred to a spawned task"
    );
    assert_eq!(pool.counters.workers_abandoned.load(Relaxed), 1);
    assert_eq!(
        pool.admission.available_permits(),
        TEST_POOL_MAX,
        "the seat must come back synchronously, not after a bounded wait"
    );
    assert!(!pool.workers.lock().unwrap().contains_key(&id));

    reap_tracked_prototype(&pool);
}

/// A tracked `PooledWorker` around a fresh shm channel, handing back the
/// worker's own end of the response ring and its notify eventfd so a test
/// can write into it as the worker would.
pub(super) fn retiring_worker_fixture(
    pool: &PoolManager,
    pid: u32,
) -> (
    PooledWorker,
    crate::ipc::shm::MappedChannel,
    std::os::fd::RawFd,
) {
    use std::os::fd::AsRawFd;

    let (fd, worker_side) = crate::ipc::shm::create_channel().unwrap();
    let mapped = crate::ipc::shm::map_existing_channel(fd).unwrap();
    let resp_efd = crate::ipc::shm::create_notify_eventfd().unwrap();
    let resp_efd_raw = resp_efd.as_raw_fd();
    let channel = crate::master::pool_manager::worker_channel::WorkerChannel::for_test(
        pid,
        Arc::new(mapped),
        crate::ipc::shm::NotifyEfds {
            req_space: crate::ipc::shm::create_notify_eventfd().unwrap(),
            resp_data: resp_efd,
        },
        unused_link(),
    );
    let id = WorkerId::next();
    let meta = Arc::new(seated_meta(pool, pid));
    pool.track_worker(id, Arc::clone(&meta));
    (
        PooledWorker {
            channel,
            pid,
            id,
            meta,
        },
        worker_side,
        resp_efd_raw,
    )
}

/// A worker that announced retirement in its `End` frame but then never
/// confirms it (e.g. wedged in a PHP shutdown function) must not be let go
/// as a routine recycle - nothing else would ever notice it is still running.
#[tokio::test]
async fn a_retiring_worker_that_never_confirms_exit_is_treated_as_wedged() {
    let mut pool = make_test_pool_manager(spawn_sleeper());
    pool.request_timeout = Some(Duration::from_millis(50));
    let pool = Arc::new(pool);
    const NO_REAL_WORKER_PID: u32 = 999_999_999;

    // Nothing is written to the ring: the marker that should follow `End`
    // never comes.
    let (worker, _worker_side, _efd) = retiring_worker_fixture(&pool, NO_REAL_WORKER_PID);
    let id = worker.id;
    let permit = Arc::clone(&pool.semaphore).try_acquire_owned().unwrap();

    Arc::clone(&pool)
        .finish_after_end(worker, permit, true, None)
        .await;

    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 1);
    assert_eq!(pool.counters.recycled_request_limit.load(Relaxed), 0);
    assert_eq!(
        pool.admission.available_permits(),
        TEST_POOL_MAX,
        "the seat must still come back even though the worker was killed, not returned"
    );
    assert!(!pool.workers.lock().unwrap().contains_key(&id));

    reap_tracked_prototype(&pool);
}

/// The mirror case: a retiring worker that does send its done marker must be
/// recycled cleanly with no signal, so the fix above does not turn every
/// ordinary request-limit recycle into a kill.
#[tokio::test]
async fn a_retiring_worker_that_confirms_exit_is_recycled_without_a_kill() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));
    const NO_REAL_WORKER_PID: u32 = 999_999_998;

    let (worker, worker_side, efd) = retiring_worker_fixture(&pool, NO_REAL_WORKER_PID);
    let id = worker.id;
    let ring = worker_side.channel();
    crate::ipc::data::write_worker_done_to_ring(&ring.response, &ring.peer_death, efd).unwrap();
    let permit = Arc::clone(&pool.semaphore).try_acquire_owned().unwrap();

    Arc::clone(&pool)
        .finish_after_end(worker, permit, true, None)
        .await;

    assert_eq!(pool.counters.recycled_request_limit.load(Relaxed), 1);
    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 0);
    assert_eq!(pool.admission.available_permits(), TEST_POOL_MAX);
    assert!(!pool.workers.lock().unwrap().contains_key(&id));

    reap_tracked_prototype(&pool);
}

/// The same wedge as above, but under the deployment default
/// (`request_timeout: None`, i.e. `limits.timeout: 0`) - the retiring branch
/// must still bound itself to `ABANDONED_KILL_GRACE`, not hold the seat
/// forever just because the ordinary watchdog is disabled.
#[tokio::test(start_paused = true)]
async fn a_retiring_worker_that_never_confirms_exit_is_bounded_with_the_watchdog_disabled() {
    let mut pool = make_test_pool_manager(spawn_sleeper());
    pool.request_timeout = None;
    let pool = Arc::new(pool);
    const NO_REAL_WORKER_PID: u32 = 999_999_997;

    let (worker, _worker_side, _efd) = retiring_worker_fixture(&pool, NO_REAL_WORKER_PID);
    let id = worker.id;
    let permit = Arc::clone(&pool.semaphore).try_acquire_owned().unwrap();

    Arc::clone(&pool)
        .finish_after_end(worker, permit, true, None)
        .await;

    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 1);
    assert_eq!(
        pool.admission.available_permits(),
        TEST_POOL_MAX,
        "the seat must come back even though `limits.timeout: 0` disabled the ordinary watchdog"
    );
    assert!(!pool.workers.lock().unwrap().contains_key(&id));

    reap_tracked_prototype(&pool);
}

/// The done marker confirms the request ended, not that the process itself
/// has exited - a worker that writes it but then hangs must still be caught
/// by the same background check `Abandoned` workers get, not trusted forever.
#[tokio::test(start_paused = true)]
async fn a_retiring_worker_that_confirms_but_never_exits_is_eventually_killed() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));
    const NO_REAL_WORKER_PID: u32 = 999_999_996;

    let (worker, worker_side, efd) = retiring_worker_fixture(&pool, NO_REAL_WORKER_PID);
    let id = worker.id;
    let ring = worker_side.channel();
    crate::ipc::data::write_worker_done_to_ring(&ring.response, &ring.peer_death, efd).unwrap();
    let permit = Arc::clone(&pool.semaphore).try_acquire_owned().unwrap();

    Arc::clone(&pool)
        .finish_after_end(worker, permit, true, None)
        .await;
    assert_eq!(pool.counters.recycled_request_limit.load(Relaxed), 1);
    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 0);
    assert!(!pool.workers.lock().unwrap().contains_key(&id));

    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(
        pool.counters.watchdog_kills.load(Relaxed),
        1,
        "a worker that never actually exits after its done marker must still get killed"
    );

    reap_tracked_prototype(&pool);
}

/// An abandoned worker confirmed gone while this is still polling (the
/// common case) must draw no kill - `retire(Abandoned)` already accounted
/// for it, so this checker's only job is catching a lie.
#[tokio::test]
async fn kill_if_still_running_leaves_a_worker_that_confirms_mid_wait_alone() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));
    const NO_REAL_WORKER_PID: u32 = 999_999_995;
    let id = tracked_worker(&pool, NO_REAL_WORKER_PID);
    let gone = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Flips well after the checker's first poll, so the assertions below
    // exercise the loop actually noticing it, not just a zero-iteration exit.
    tokio::spawn({
        let gone = Arc::clone(&gone);
        async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            gone.store(true, Relaxed);
        }
    });

    super::dispatch::kill_if_still_running(
        Arc::clone(&pool),
        WorkerRef {
            id,
            pid: NO_REAL_WORKER_PID,
        },
        gone,
        Duration::from_secs(60),
    )
    .await;

    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 0);
    assert!(
        pool.workers.lock().unwrap().contains_key(&id),
        "confirmed-gone must not touch tracking - that already happened elsewhere"
    );

    reap_tracked_prototype(&pool);
}

/// An abandoned worker that never confirms exit within `grace` (still
/// running PHP with nothing pending on the ring) must not be trusted
/// forever - it is wedged, like any other worker that missed a deadline.
#[tokio::test]
async fn kill_if_still_running_kills_a_worker_that_never_confirms() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));
    const NO_REAL_WORKER_PID: u32 = 999_999_994;
    let id = tracked_worker(&pool, NO_REAL_WORKER_PID);
    let gone = Arc::new(std::sync::atomic::AtomicBool::new(false));

    super::dispatch::kill_if_still_running(
        Arc::clone(&pool),
        WorkerRef {
            id,
            pid: NO_REAL_WORKER_PID,
        },
        gone,
        Duration::from_millis(50),
    )
    .await;

    assert_eq!(pool.counters.watchdog_kills.load(Relaxed), 1);
    assert!(
        !pool.workers.lock().unwrap().contains_key(&id),
        "a wedged worker that gets killed must not stay tracked"
    );

    reap_tracked_prototype(&pool);
}

/// Every reason has a counter of its own, except the one whose caller knows
/// better than the pool whether the request ultimately failed.
#[tokio::test]
async fn each_reason_counts_under_its_own_name() {
    let pool = make_test_pool_manager(spawn_sleeper());
    const NO_REAL_WORKER_PID: u32 = 999_999_999;

    let counters = |p: &PoolManager| {
        [
            p.counters.recycled_idle_timeout.load(Relaxed),
            p.counters.recycled_request_limit.load(Relaxed),
            p.counters.workers_abandoned.load(Relaxed),
            p.counters.watchdog_kills.load(Relaxed),
            p.counters.dispatch_failed.load(Relaxed),
            p.counters.workers_vanished_idle.load(Relaxed),
        ]
    };

    for (why, expected) in [
        (Retired::IdleTimeout, [1, 0, 0, 0, 0, 0]),
        (Retired::RequestLimit, [1, 1, 0, 0, 0, 0]),
        (Retired::Abandoned, [1, 1, 1, 0, 0, 0]),
        (Retired::Watchdog, [1, 1, 1, 1, 0, 0]),
        (Retired::Failed, [1, 1, 1, 1, 1, 0]),
        (Retired::Vanished, [1, 1, 1, 1, 1, 1]),
        // Nothing moves: the caller accounts for this one.
        (Retired::Unavailable, [1, 1, 1, 1, 1, 1]),
    ] {
        let id = tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(
            WorkerRef {
                id,
                pid: NO_REAL_WORKER_PID,
            },
            why,
        );
        assert_eq!(counters(&pool), expected, "after {:?}", why.as_str());
    }
    reap_tracked_prototype(&pool);
}

/// The prototype's own death is the only one that triggers a respawn; a
/// worker reparented here by `PR_SET_CHILD_SUBREAPER` must not.
#[tokio::test]
async fn only_the_prototypes_own_exit_is_reported_as_its_death() {
    let prototype_pid = spawn_sleeper();
    let pool = make_test_pool_manager(prototype_pid);
    let mut handle = pool.prototype_child.lock().unwrap();

    let stray = WaitStatus::Exited(Pid::from_raw(prototype_pid as i32 + 1), 0);
    assert!(
        !handle.note_exit(prototype_pid + 1, stray),
        "another child's exit is not the prototype's"
    );

    let own = WaitStatus::Exited(Pid::from_raw(prototype_pid as i32), 0);
    assert!(handle.note_exit(prototype_pid, own));
    assert!(
        !handle.note_exit(prototype_pid, own),
        "a second report of the same death would respawn twice"
    );
    drop(handle);

    // Already accounted for above, so nothing is left to kill.
    assert_eq!(pool.prototype_child.lock().unwrap().live_pid(), None);
    let pid = Pid::from_raw(prototype_pid as i32);
    let _ = kill(pid, Signal::SIGKILL);
    let _ = waitpid(pid, None);
}

/// A prototype reaped by the sweep and then replaced must not be signalled on
/// the way out: `replace` would otherwise `waitpid` and `kill` a pid the
/// kernel may already have handed to something unrelated.
#[tokio::test]
async fn replacing_a_prototype_already_reaped_by_the_sweep_never_signals_it() {
    let squatter_pid = spawn_sleeper() as i32;
    let pool = make_test_pool_manager(squatter_pid as u32);
    // Stands in for the sweep having reaped it, with a live process still on
    // the number - as a recycled pid would be.
    pool.prototype_child.lock().unwrap().note_exit(
        squatter_pid as u32,
        WaitStatus::Exited(Pid::from_raw(squatter_pid), 0),
    );

    pool.prototype_child
        .lock()
        .unwrap()
        .replace(spawn_sleeper());

    assert!(
        !became_a_zombie(squatter_pid).await,
        "a reaped pid was signalled while being replaced"
    );
    let pid = Pid::from_raw(squatter_pid);
    let _ = kill(pid, Signal::SIGKILL);
    let _ = waitpid(pid, None);
    reap_tracked_prototype(&pool);
}

/// At `processes.max` a caller already holds a permit, so a worker is owed to
/// it and only a return can deliver one. Waiting for that return rather than
/// polling for it is the difference between microseconds and a timer tick.
#[tokio::test]
async fn a_caller_waiting_at_the_ceiling_is_woken_by_a_returned_worker() {
    let pool = Arc::new(make_test_pool_manager(spawn_sleeper()));

    // Every seat spoken for, so `spawn_worker` can only answer "pool full".
    let parked = idle_worker(&pool, 4242, false);
    let parked_id = parked.id;
    let _held: Vec<_> =
        std::iter::from_fn(|| Arc::clone(&pool.admission).try_acquire_owned().ok()).collect();

    let waiter = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.get_worker().await.map(|w| (w.id, w.pid)) })
    };
    // Long enough that the waiter is parked on the notify, not still on its
    // first pop.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "nothing to hand out yet");

    pool.return_worker(parked);

    let (id, pid) = tokio::time::timeout(Duration::from_millis(250), waiter)
        .await
        .expect("a returned worker must reach the waiter, not leave it to time out")
        .unwrap()
        .expect("the returned worker is the one it gets");
    assert_eq!(
        id, parked_id,
        "the waiter must get the exact worker that was parked, not merely one with the same pid"
    );
    assert_eq!(pid, 4242);

    // `Vanished`, not `IdleTimeout`: 4242 is a placeholder pid, not a real
    // process, and `IdleTimeout` now signals it.
    pool.retire(WorkerRef { id, pid }, Retired::Vanished);
    reap_tracked_prototype(&pool);
}

/// The prototype reaps workers, so a pid master remembers may already be reused.
#[tokio::test]
async fn retiring_a_worker_never_signals_its_pid_from_master() {
    let pool = make_test_pool_manager(spawn_sleeper());
    // An unrelated process that got the reaped worker's pid.
    let squatter = spawn_sleeper();
    let id = tracked_worker(&pool, squatter);

    pool.retire(WorkerRef { id, pid: squatter }, Retired::Failed);

    assert!(
        !became_a_zombie(squatter as i32).await,
        "master signalled a pid it does not own"
    );
    let pid = Pid::from_raw(squatter as i32);
    let _ = kill(pid, Signal::SIGKILL);
    let _ = waitpid(pid, None);
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn a_retired_worker_is_killed_through_the_prototype() {
    let (pool, prototype_end) = make_test_pool_manager_with_control(spawn_sleeper());
    let id = tracked_worker(&pool, 4242);

    pool.retire(WorkerRef { id, pid: 4242 }, Retired::Watchdog);

    assert_eq!(kills_sent(&prototype_end).await, vec![4242]);
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn a_pool_is_settled_only_once_nothing_is_left_for_a_pass_to_do() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let now = Instant::now();
    pool.idle
        .lock()
        .unwrap()
        .push_back(idle_worker(&pool, 601, false));

    assert_eq!(
        pool.settled_until(1, now),
        None,
        "activity since the last pass"
    );
    pool.active.store(false, Relaxed);
    assert_eq!(pool.settled_until(1, now), Some(now + SETTLED_RECHECK));
    assert_eq!(pool.settled_until(2, now), None, "a spare still to top up");

    pool.idle
        .lock()
        .unwrap()
        .push_back(idle_worker(&pool, 602, true));
    assert_eq!(
        pool.settled_until(1, now),
        None,
        "a vanished worker to retire"
    );
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn a_settled_pool_wakes_for_the_next_idle_timeout_above_spare() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let now = Instant::now();
    let pool = PoolManager {
        idle_timeout: Some(Duration::from_secs(60)),
        started_at: now - Duration::from_secs(10),
        ..pool
    };
    for pid in [701u32, 702] {
        pool.idle
            .lock()
            .unwrap()
            .push_back(idle_worker(&pool, pid, false));
    }
    pool.active.store(false, Relaxed);

    assert_eq!(
        pool.settled_until(1, now),
        Some(now + Duration::from_secs(50))
    );
    assert_eq!(
        pool.settled_until(2, now),
        Some(now + SETTLED_RECHECK),
        "nothing above spare to time out"
    );
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn the_first_activity_after_settling_wakes_maintenance() {
    let pool = make_test_pool_manager(spawn_sleeper());
    pool.active.store(false, Relaxed);
    let woken = pool.maintenance.notified();

    let _ = pool.take_idle();

    tokio::time::timeout(Duration::from_secs(1), woken)
        .await
        .expect("take_idle on a settled pool must wake maintenance");
    assert!(pool.active.load(Relaxed));
    reap_tracked_prototype(&pool);
}

#[tokio::test]
async fn a_worker_returning_to_a_settled_pool_wakes_maintenance_but_a_sweep_does_not() {
    let pool = make_test_pool_manager(spawn_sleeper());
    pool.idle
        .lock()
        .unwrap()
        .push_back(idle_worker(&pool, 801, false));
    pool.active.store(false, Relaxed);

    pool.sweep_idle_workers(1).await;
    assert!(
        !pool.active.load(Relaxed),
        "a sweep alone must let the pool settle"
    );

    let worker = pool.idle.lock().unwrap().pop_front().unwrap();
    pool.return_worker(worker);
    assert!(
        pool.active.load(Relaxed),
        "a returned worker can put the pool above spare"
    );
    reap_tracked_prototype(&pool);
}
