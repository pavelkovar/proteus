use super::*;

/// Enough real state for the prototype-lifecycle tests, which touch only the
/// child handle and the counters. The child must be a real killable process;
/// which one does not matter.
fn make_test_pool_manager(prototype_child: std::process::Child) -> PoolManager {
    let (control_sock, _unused_other_end) = UnixSeqpacket::pair().unwrap();
    PoolManager {
        control: Mutex::new(control_sock),
        idle: IdleStack::new(4),
        semaphore: Arc::new(Semaphore::new(1)),
        max_workers: 1,
        request_timeout: Duration::from_secs(30),
        queue_timeout: Duration::from_secs(5),
        spawn_timeout: Duration::from_secs(30),
        idle_timeout: 0,
        queue_max_depth: 0,
        queue_depth: AtomicU64::new(0),
        started_at: Instant::now(),
        target_names: Vec::new(),
        requests_total: AtomicU64::new(0),
        watchdog_kills: AtomicU64::new(0),
        queue_timeouts: AtomicU64::new(0),
        dispatch_failed: AtomicU64::new(0),
        workers_spawned: AtomicU64::new(0),
        recycled_request_limit: AtomicU64::new(0),
        recycled_idle_timeout: AtomicU64::new(0),
        prototype_child: StdMutex::new(PrototypeHandle::new(prototype_child)),
        php_mod_path: String::new(),
        max_requests: 500,
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
        drop_privileges: false,
        no_new_privs: true,
        options: PhpOptions::default(),
        environment: HashMap::new(),
        respawn_backoff: Mutex::new(RespawnBackoff::default()),
        crash_loop_backoffs: AtomicU64::new(0),
        prototype_respawns: AtomicU64::new(0),
        workers: StdMutex::new(HashMap::new()),
    }
}

/// A zero max takes the unbounded branch and must never reject, however many
/// slots are held at once.
#[test]
fn queue_depth_guard_with_zero_max_never_rejects() {
    let counter = AtomicU64::new(0);
    let guards: Vec<_> = (0..10_000)
        .map(|_| QueueDepthGuard::try_new(&counter, 0).expect("max=0 must never reject"))
        .collect();
    assert_eq!(counter.load(Relaxed), 10_000);
    drop(guards);
    assert_eq!(counter.load(Relaxed), 0);
}

/// A real cap must reject once full and admit again once a slot frees.
#[test]
fn queue_depth_guard_rejects_once_a_real_cap_is_reached() {
    let counter = AtomicU64::new(0);
    let first = QueueDepthGuard::try_new(&counter, 2).expect("slot 1 of 2");
    let second = QueueDepthGuard::try_new(&counter, 2).expect("slot 2 of 2");
    assert!(
        QueueDepthGuard::try_new(&counter, 2).is_none(),
        "a 3rd slot must be rejected at max=2"
    );

    drop(first);
    let _third =
        QueueDepthGuard::try_new(&counter, 2).expect("a freed slot must be admitted again");
    drop(second);
}

/// The slot must be released when the holding future is cancelled mid-wait,
/// not only on a normal return. Shaped like the real caller, with the guard
/// alive across an `.await` that never completes.
#[tokio::test]
async fn queue_depth_guard_releases_its_slot_when_the_waiting_future_is_cancelled() {
    let counter = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(Semaphore::new(0)); // never has a free permit to hand out

    let counter_task = Arc::clone(&counter);
    let semaphore_task = Arc::clone(&semaphore);
    let task = tokio::spawn(async move {
        let guard =
            QueueDepthGuard::try_new(&counter_task, 0).expect("unlimited depth always admits");
        let _permit = semaphore_task.acquire_owned().await;
        drop(guard); // unreachable: the semaphore never yields a permit
    });

    // Let it park on the await before cancelling.
    tokio::task::yield_now().await;
    assert_eq!(
        counter.load(Relaxed),
        1,
        "guard should have incremented the counter before parking"
    );

    task.abort();
    let _ = task.await;

    assert_eq!(
        counter.load(Relaxed),
        0,
        "QueueDepthGuard must release its slot even when cancelled mid-await, or queue_depth leaks forever"
    );
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
/// answering. `Child` does not kill on drop, so overwriting the handle would
/// orphan it: alive, holding its PHP heap, referenced and reaped by nobody.
#[tokio::test]
async fn replacing_a_still_running_prototype_kills_and_reaps_it() {
    let old = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .unwrap();
    let old_pid = old.id() as i32;
    let pool = make_test_pool_manager(old);

    let replacement = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .unwrap();
    let new_pid = replacement.id();
    pool.replace_prototype_child(replacement);

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
        "the new child must be the tracked one"
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
    let squatter = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .unwrap();
    let squatter_pid = squatter.id() as i32;
    let pool = make_test_pool_manager(squatter);
    pool.prototype_child.lock().unwrap().mark_reaped();

    pool.kill_prototype();

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
    // Exit first, so this takes the already-reaped branch rather than racing it.
    while !matches!(old.try_wait(), Ok(Some(_))) {
        std::thread::sleep(Duration::from_millis(10));
    }
    // Already reaped above; hand the pool a fresh handle to that same state.
    let pool = make_test_pool_manager(old);

    let replacement = std::process::Command::new("sleep")
        .arg("100")
        .spawn()
        .unwrap();
    pool.replace_prototype_child(replacement);

    assert!(
        kill(Pid::from_raw(old_pid), None).is_err(),
        "an exited prototype must not linger"
    );

    reap_tracked_prototype(&pool);
}
