use super::*;
use nix::sys::signal::kill;
use nix::sys::wait::{WaitStatus, waitpid};

/// How many workers the fixture pool admits; the lifecycle tests lean on the
/// seat coming back, so they need room for more than one at a time.
const TEST_POOL_MAX: usize = 4;

/// Enough real state for the lifecycle tests, which touch only the prototype
/// handle, the admission seats and the counters. `prototype_pid` must name a
/// real killable process, since these tests signal and reap it.
fn make_test_pool_manager(prototype_pid: u32) -> PoolManager {
    let (control_sock, _unused_other_end) = UnixSeqpacket::pair().unwrap();
    PoolManager {
        control: Mutex::new(control_sock),
        control_rt: tokio::runtime::Handle::current(),
        idle: StdMutex::new(VecDeque::new()),
        worker_returned: Notify::new(),
        semaphore: Arc::new(Semaphore::new(1)),
        admission: Arc::new(Semaphore::new(TEST_POOL_MAX)),
        max_workers: 1,
        request_timeout: Duration::from_secs(30),
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
    }
}

/// A real, killable process standing in for a prototype or a worker; which
/// one it is does not matter.
// The `Child` is dropped unwaited on purpose: these tests reap through
// `waitpid`, as master does, and a second reaper would race it.
#[allow(clippy::zombie_processes)]
fn spawn_sleeper() -> u32 {
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

/// The busy/idle bookkeeping runs with no lock and no pid lookup; this pins
/// that what `status_json` reports is still exact.
#[test]
fn worker_meta_tracks_state_and_request_count_without_the_pool_lock() {
    let pool_started = Instant::now();
    let meta = detached_meta(pool_started);
    assert_eq!(meta.state_str(), "idle");
    assert_eq!(meta.request_count.load(Relaxed), 0);

    meta.mark_busy(Instant::now(), pool_started);
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

    meta.mark_busy(Instant::now(), pool_started);
    assert_eq!(meta.request_count.load(Relaxed), 2);
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
                    meta.mark_busy(Instant::now(), pool_started);
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

fn reap_tracked_prototype(pool: &PoolManager) {
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
        meta: Arc::new(seated_meta(pool)),
    }
}

/// A `WorkerMeta` holding one of `pool`'s admission seats, as a real spawn
/// gives it.
fn seated_meta(pool: &PoolManager) -> WorkerMeta {
    WorkerMeta::new(
        Arc::clone(&pool.admission)
            .try_acquire_owned()
            .expect("the fixture pool has a free seat"),
        pool.started_at,
        pool.started_at,
    )
}

/// For the tests that exercise `WorkerMeta` on its own, with no pool behind it.
fn detached_meta(pool_started: Instant) -> WorkerMeta {
    let seats = Arc::new(Semaphore::new(1));
    WorkerMeta::new(
        seats.try_acquire_owned().expect("a fresh semaphore"),
        pool_started,
        pool_started,
    )
}

/// A worker that retired itself must be found wherever it is parked. Dispatch
/// only ever sees the back, so anything the sweep fails to rotate past is
/// reported idle forever.
#[tokio::test]
async fn a_retired_worker_is_reaped_from_any_position() {
    for position in 0..3 {
        let pool = make_test_pool_manager(spawn_sleeper());
        for (offset, pid) in [101u32, 102, 103].into_iter().enumerate() {
            let worker = idle_worker(&pool, pid, offset == position);
            pool.idle.lock().unwrap().push_back(worker);
        }

        pool.sweep_idle_workers().await;

        let left: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
        assert_eq!(
            left.len(),
            2,
            "the retired worker at position {position} was never looked at"
        );
        assert!(!left.contains(&(101 + position as u32)));
        assert_eq!(pool.counters.recycled_idle_timeout.load(Relaxed), 1);
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

    pool.sweep_idle_workers().await;

    let after: Vec<u32> = pool.idle.lock().unwrap().iter().map(|w| w.pid).collect();
    assert_eq!(after, vec![201, 202, 203, 204]);
    reap_tracked_prototype(&pool);
}

/// Workers are tracked by pid, and the OS reuses pids. An entry displaced by
/// a new worker under the same pid must give its seat up with it, or the
/// pool's ceiling drops by one for good every time that happens.
#[tokio::test]
async fn reusing_a_pid_that_was_never_reaped_gives_its_seat_back() {
    let pool = make_test_pool_manager(spawn_sleeper());
    assert_eq!(pool.admission.available_permits(), TEST_POOL_MAX);

    // A worker that died without anyone noticing: still tracked, still seated.
    const PID: u32 = 4242;
    pool.track_worker(PID, Arc::new(seated_meta(&pool)));
    assert_eq!(pool.admission.available_permits(), TEST_POOL_MAX - 1);

    // The same pid comes back on a fresh worker, through the path a real
    // spawn takes.
    pool.track_worker(PID, Arc::new(seated_meta(&pool)));

    assert_eq!(
        pool.admission.available_permits(),
        TEST_POOL_MAX - 1,
        "the displaced entry kept its seat, so the pool is one worker poorer"
    );
    assert_eq!(pool.counters.workers_reaped_dead.load(Relaxed), 1);
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

    let respawned = pool.try_respawn_prototype().await;

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
fn tracked_worker(pool: &PoolManager, pid: u32) {
    pool.track_worker(pid, Arc::new(seated_meta(pool)));
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
        Retired::Unavailable,
    ] {
        // Out of range, so the kill variants fail harmlessly with ESRCH.
        const NO_REAL_WORKER_PID: u32 = 999_999_999;
        tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(NO_REAL_WORKER_PID, why);
        assert_eq!(
            pool.admission.available_permits(),
            TEST_POOL_MAX,
            "retiring for {:?} kept the seat",
            why.as_str()
        );
        assert!(
            !pool
                .workers
                .lock()
                .unwrap()
                .contains_key(&NO_REAL_WORKER_PID),
            "a retired worker is still tracked after {:?}",
            why.as_str()
        );
    }
    reap_tracked_prototype(&pool);
}

/// A worker that is retiring itself is already on its way out; signalling it
/// would race a pid master no longer owns. One that is merely unwanted has to
/// be stopped, or it runs on holding its PHP heap with nothing tracking it.
#[tokio::test]
async fn only_the_reasons_that_leave_a_worker_running_kill_it() {
    let pool = make_test_pool_manager(spawn_sleeper());

    for (why, expect_killed) in [
        (Retired::IdleTimeout, false),
        (Retired::RequestLimit, false),
        (Retired::Abandoned, false),
        (Retired::Watchdog, true),
        (Retired::Failed, true),
        (Retired::Unavailable, true),
    ] {
        let worker_pid = spawn_sleeper();
        tracked_worker(&pool, worker_pid);

        pool.retire(worker_pid, why);

        assert_eq!(
            became_a_zombie(worker_pid as i32).await,
            expect_killed,
            "wrong kill decision for {:?}",
            why.as_str()
        );
        let pid = Pid::from_raw(worker_pid as i32);
        let _ = kill(pid, Signal::SIGKILL);
        let _ = waitpid(pid, None);
    }
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
        ]
    };

    for (why, expected) in [
        (Retired::IdleTimeout, [1, 0, 0, 0, 0]),
        (Retired::RequestLimit, [1, 1, 0, 0, 0]),
        (Retired::Abandoned, [1, 1, 1, 0, 0]),
        (Retired::Watchdog, [1, 1, 1, 1, 0]),
        (Retired::Failed, [1, 1, 1, 1, 1]),
        // Nothing moves: the caller accounts for this one.
        (Retired::Unavailable, [1, 1, 1, 1, 1]),
    ] {
        tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(NO_REAL_WORKER_PID, why);
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
    let _held: Vec<_> =
        std::iter::from_fn(|| Arc::clone(&pool.admission).try_acquire_owned().ok()).collect();

    let waiter = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.get_worker().await.map(|w| w.pid) })
    };
    // Long enough that the waiter is parked on the notify, not still on its
    // first pop.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "nothing to hand out yet");

    pool.return_worker(parked);

    let got = tokio::time::timeout(Duration::from_millis(250), waiter)
        .await
        .expect("a returned worker must reach the waiter, not leave it to time out")
        .unwrap()
        .expect("the returned worker is the one it gets");
    assert_eq!(got, 4242);

    pool.retire(4242, Retired::IdleTimeout);
    reap_tracked_prototype(&pool);
}
