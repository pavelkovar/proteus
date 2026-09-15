use super::*;

/// Enough real state for the lifecycle tests, which touch only the prototype
/// handle, the slots and the counters. `prototype_pid` must name a real
/// killable process, since these tests signal and reap it.
fn make_test_pool_manager(prototype_pid: u32) -> PoolManager {
    let (control_sock, _unused_other_end) = UnixSeqpacket::pair().unwrap();
    PoolManager {
        control: Mutex::new(control_sock),
        control_rt: tokio::runtime::Handle::current(),
        idle: IdleStack::new(4),
        semaphore: Arc::new(Semaphore::new(1)),
        max_workers: 1,
        request_timeout: Duration::from_secs(30),
        queue_timeout: Duration::from_secs(5),
        spawn_timeout: Duration::from_secs(30),
        queue_max_depth: 0,
        queue_depth: Gauge::default(),
        started_at: Instant::now(),
        target_names: Vec::new(),
        counters: Counters::default(),
        prototype_child: StdMutex::new(PrototypeHandle::new(prototype_pid)),
        prototype_spec: prototype_launch::PrototypeSpec {
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
    let meta = WorkerMeta::new(0, pool_started, pool_started);
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
    let meta = Arc::new(WorkerMeta::new(0, pool_started, pool_started));
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
    let channel = crate::master::worker_channel::WorkerChannel::for_test(
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
    let slot = pool.idle.claim_slot().expect("a free slot");
    PooledWorker {
        channel,
        pid,
        meta: Arc::new(WorkerMeta::new(slot, pool.started_at, pool.started_at)),
    }
}

/// The idle stack is LIFO, so a worker pushed straight back is the next one
/// popped. A sweep that does that only ever sees the top of the stack, and a
/// worker that retired underneath it is reported idle forever.
#[tokio::test]
async fn a_retired_worker_below_the_top_of_the_idle_stack_is_still_reaped() {
    let pool = make_test_pool_manager(spawn_sleeper());

    // Pushed first, so it ends up at the bottom; the two live ones cover it.
    for (pid, retired) in [(101, true), (102, false), (103, false)] {
        let worker = idle_worker(&pool, pid, retired);
        pool.idle.push(worker);
    }
    assert_eq!(pool.idle.len(), 3);

    pool.sweep_idle_workers().await;

    assert_eq!(
        pool.idle.len(),
        2,
        "the retired worker under the live ones was never looked at"
    );
    assert_eq!(pool.counters.recycled_idle_timeout.load(Relaxed), 1);
    reap_tracked_prototype(&pool);
}

/// Workers are tracked by pid, and the OS reuses pids. An entry displaced by
/// a new worker takes its slot with it unless the slot is handed back, and the
/// pool's ceiling drops by one for good every time that happens.
#[tokio::test]
async fn reusing_a_pid_that_was_never_reaped_gives_its_slot_back() {
    let pool = make_test_pool_manager(spawn_sleeper());
    let free_at_rest = pool.idle.claim_slot().map(|s| {
        pool.idle.release_slot(s);
        s
    });
    assert!(
        free_at_rest.is_some(),
        "the fixture must start with a free slot"
    );

    // A worker that died without anyone noticing: its entry is still tracked
    // and its slot is out of circulation.
    const PID: u32 = 4242;
    let stale_slot = pool.idle.claim_slot().expect("a free slot");
    pool.track_worker(
        PID,
        Arc::new(WorkerMeta::new(
            stale_slot,
            pool.started_at,
            pool.started_at,
        )),
    );

    // The same pid comes back on a fresh worker, through the path a real
    // spawn takes.
    let new_slot = pool.idle.claim_slot().expect("a second free slot");
    pool.track_worker(
        PID,
        Arc::new(WorkerMeta::new(new_slot, pool.started_at, pool.started_at)),
    );

    // Both slots must be reachable again once the new worker gives its own up.
    pool.idle.release_slot(new_slot);
    let mut reclaimed = Vec::new();
    while let Some(s) = pool.idle.claim_slot() {
        reclaimed.push(s);
    }
    assert!(
        reclaimed.contains(&stale_slot),
        "the displaced entry's slot never came back: {reclaimed:?}"
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

/// A live worker the pool accounts for, holding a slot as a real one does.
fn tracked_worker(pool: &PoolManager, pid: u32) -> u32 {
    let slot = pool.idle.claim_slot().expect("a free slot");
    pool.track_worker(
        pid,
        Arc::new(WorkerMeta::new(slot, pool.started_at, pool.started_at)),
    );
    slot
}

/// Every retirement path gives the slot back. Leaking one lowers the pool's
/// real ceiling for good, and the reason it left must not change that.
#[tokio::test]
async fn every_retirement_reason_returns_the_slot() {
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
        let slot = tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(NO_REAL_WORKER_PID, why);
        assert_eq!(
            pool.idle.claim_slot(),
            Some(slot),
            "retiring for {:?} kept the slot",
            why.as_str()
        );
        pool.idle.release_slot(slot);
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
        let slot = tracked_worker(&pool, worker_pid);

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
        pool.idle.release_slot(slot);
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
        let slot = tracked_worker(&pool, NO_REAL_WORKER_PID);
        pool.retire(NO_REAL_WORKER_PID, why);
        pool.idle.release_slot(slot);
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
